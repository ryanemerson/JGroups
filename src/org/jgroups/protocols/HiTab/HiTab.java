package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.concurrent.*;

/**
 * // TODO: Document this
 *
 * @author ryanemerson
 * @since 4.0
 */
public class HiTab extends Protocol {

    @Property(name = "ack_wait", description = "How long, in milliseconds, the system waits to receive an acknowlegment "
              + "before delivering a message.  This value is equal to the minimum broadcast rate allowed by the system")
    private int ackWait = 2000;

    @Property(name = "number_of_old_messages", description = "The minimum number of old messages that should be stored by " +
             "HiTab.  A large value increases the chance that a lost message will be recoverable.  A smaller value " +
             "reduces the protocols memory consumption.")
    private int numberOfSequences = 100;

    @Property(name = "garbage_collection", description = "How often, in minutes, the system performs garbage collection of old messages")
    private int garbageCollectionRate = 10;

    private HiTabBuffer buffer;
    private Address localAddress = null;
    private TimeScheduler timer;
    private Executor threadPool;

    private View view;

    private volatile long sequence = 0L;
    final private Map<MessageId, Message> messageStore = new ConcurrentHashMap<MessageId, Message>();
    final private Map<MessageId, Boolean> requestStatus = new ConcurrentHashMap<MessageId, Boolean>();
    final private BlockingQueue<MessageId> acks = new ArrayBlockingQueue<MessageId>(20, true);
    private ExecutorService executor;

    public HiTab() {
    }

    @Override
    public void init() throws Exception{
        timer = getTransport().getTimer();
        buffer = new HiTabBuffer(this, ackWait);
        threadPool = getTransport().getDefaultThreadPool();
    }

    @Override
    public void start() throws Exception {
        super.start();
        // Thread dedicated to delivering messages.  Required, so must be separate from the default thread pool
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());
        timer.scheduleWithFixedDelay(new GarbageCollection(), garbageCollectionRate, garbageCollectionRate, TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                if (message.isFlagSet(Message.Flag.OOB))
                    return up_prot.up(event);

                HiTabHeader header = (HiTabHeader) message.getHeader(this.id);
                if (header == null) {
                    return up_prot.up(event);
                }

                switch (header.getType()) {
                    case HiTabHeader.RETRANSMISSION:
                        requestStatus.put(header.getId(), true);
                        if (messageStore.containsKey(header.getId()))
                            break;
                    case HiTabHeader.BROADCAST:
                        messageStore.put(header.getId(), message);
                        if (buffer.addMessage(message, view))
                            ackMessage(header.getId());
                        break;
                    case HiTabHeader.PLACEHOLDER_REQUEST:
                        handlePlaceholdeRequest(header);
                        break;
                    case HiTabHeader.SEQUENCE_REQUEST:
                        handleSequenceRequest(header);
                        break;
                }
                // Return null so that the up event only occurrs if a message has been delivered from the buffer
                // or its not HiTab message or is OOB
                return null;
            case Event.VIEW_CHANGE:
                view = (View) event.getArg();
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                if (message.isFlagSet(Message.Flag.OOB))
                    break;

                sendMessage(message);
                return down_prot.down(event);
            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);
        }
        return down_prot.down(event);
    }

    public void sendPlaceholderRequest(MessageId id, Address ackInformer) {
        HiTabHeader header = HiTabHeader.createPlaceholder(id, ackInformer);
        sendRequest(header);
    }

    public void sendSequenceRequest(Address origin, long sequence) {
        HiTabHeader header = HiTabHeader.createSequenceRequest(origin, sequence);
        sendRequest(header);
    }

    public long getCurrentTime() {
        return (Long) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_TIME)));
    }

    public NMCData getNMCData() {
        return (NMCData) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES)));
    }

    private void deliverMessage(Message message) {
        up_prot.up(new Event(Event.MSG, message));
    }

    private void sendRequest(HiTabHeader header) {
        Message message = new Message();
        message.putHeader(this.id, header);
        message.setFlag(Message.Flag.DONT_BUNDLE);
        down_prot.down(new Event(Event.MSG, message));
    }

    private void sendMessage(Message message) {
        NMCData data = getNMCData();
        MessageId id = new MessageId(getCurrentTime(), localAddress, sequence++);
        HiTabHeader header = new HiTabHeader(id, HiTabHeader.BROADCAST, data.getCapD(), data.getCapS(), data.getXMax(), localAddress, getMessageAcks());
        message.putHeader(this.id, header);
        message.setFlag(Message.Flag.DONT_BUNDLE);
    }

    private void resendMessage(MessageId id) {
        Message message = messageStore.get(id);
        resendMessage(message);
    }

    // No need to reset flags as they should still be in place from the original broadcast
    private void resendMessage(Message message) {
        HiTabHeader header = (HiTabHeader) message.getHeader(this.id);
        header.setType(HiTabHeader.RETRANSMISSION);
        down_prot.down(new Event(Event.MSG, message));
    }

    private void handleSequenceRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage = getMessageBySequence(id);
        if (requestedMessage == null)
            return;
        // Reload the actual id, so that the equals method will correctly return true when searching our records
        // This is necessary because the sequence request will be missing the originator field
        // Therefore the equals method will return false if the above id is used, even if we have the message
        id = ((HiTabHeader)requestedMessage.getHeader(this.id)).getId();
        if (validRequest(id)) {
            requestStatus.put(id, false);
            if (id.getOriginator().equals(localAddress)) {
                resendMessage(id);
                requestStatus.remove(id);
            } else {
                threadPool.execute(new RequestTimeout(requestedMessage));
            }
        }

    }

    private void handlePlaceholdeRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage = messageStore.get(id);
        if (requestedMessage == null) {
            buffer.addPlaceholder(header.getAckInformer(), id);
            return;
        }

        if (validRequest(id)) {
            requestStatus.put(id, false);
            if (header.getAckInformer().equals(localAddress)) {
                resendMessage(id);
                requestStatus.remove(id);
            } else {
                threadPool.execute(new RequestTimeout(requestedMessage));
            }
        }
    }

    private boolean validRequest(MessageId id) {
        if (!requestSatisfied(id))
            return false;

        boolean broadcastComplete = (Boolean) down_prot.down(new Event(Event.USER_DEFINED,
                new HiTabEvent(HiTabEvent.BROADCAST_COMPLETE, id)));
        if (!broadcastComplete)
            return false;
        return true;
    }

    private Message getMessageBySequence(MessageId id) {
        for (MessageId msgId : messageStore.keySet()) {
            if (id.getSequence() == msgId.getSequence() && id.getOriginator().equals(msgId.getOriginator()))
                return messageStore.get(msgId);
        }
        return null;
    }

    // Returns true if a request status cannot be found, as this means that the request has been satisfied!
    private boolean requestSatisfied(MessageId id) {
        Boolean satisfied = requestStatus.get(id);
        if (satisfied == null) {
            return true;
        }
        return satisfied;
    }


    private void ackMessage(MessageId id) {
        synchronized (acks) {
            if (!acks.contains(id)) {
                try {
                    acks.put(id);
                } catch (InterruptedException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }
            }
        }
    }

    private List<MessageId> getMessageAcks() {
        List<MessageId> ackList = new ArrayList<MessageId>();
        acks.drainTo(ackList);
        return ackList;
    }

    private void collectGarbage() {
        List<MessageId> garbage = new ArrayList<MessageId>();
        synchronized (messageStore) {
            Iterator<MessageId> i = messageStore.keySet().iterator();
            while (i.hasNext()) {
                MessageId id = i.next();
                long seqDifference = buffer.getSequence(id.getOriginator()) - id.getSequence();
                boolean broadcastComplete = (Boolean) down_prot.down(new Event(Event.USER_DEFINED,
                        new HiTabEvent(HiTabEvent.BROADCAST_COMPLETE, id)));
                if (seqDifference > numberOfSequences && broadcastComplete) {
                    i.remove();
                    garbage.add(id);
                }
            }
        }
        if (garbage.size() > 0)
            down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.COLLECT_GARBAGE, garbage)));
    }

    final class RequestTimeout implements Runnable {
        final private Message message;
        RequestTimeout(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            NMCData data = getNMCData();
            try {
                int delay = (int) Math.ceil(data.getOmega() + data.getXMax());
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }

            MessageId id = ((HiTabHeader) message.getHeader(HiTab.this.id)).getId();
            if (requestSatisfied(id))
                requestStatus.remove(id);
            else {
                Random r = new Random();
                int delay = r.nextInt((int) Math.ceil(data.getEta()));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                }

                if (requestSatisfied(id))
                    requestStatus.remove(id);
                else {
                    resendMessage(message);
                }
            }
        }
    }

    final class DeliverMessages implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    List<Message> messages = buffer.process();
                    for (Message message : messages) {
                        deliverMessage(message);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    final class GarbageCollection implements Runnable {
        @Override
        public void run(){
            collectGarbage();
        }
    }
}