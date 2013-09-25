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
import java.util.concurrent.atomic.AtomicInteger;

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

    @Property(name = "request_timeout", description = "The amount of time in milliseconds for the buffer to wait after sending a message request")
    private int requestTimeout = 50;

    private HiTabBuffer buffer;
    public Address localAddress = null; // TODO CHANGE TO PRIVATE
    private TimeScheduler timer;

    private View view;

    private volatile long lastBroadcastTime = 0L;
    private volatile Future<?> sendBlankMessage = null;
    final private Map<MessageId, Message> messageStore = new ConcurrentHashMap<MessageId, Message>();
    final private BlockingQueue<MessageId> acks = new ArrayBlockingQueue<MessageId>(20, true);
    final private Map<MessageId, Long> requestStatus = new ConcurrentHashMap<MessageId, Long>();
    final private Map<MessageId, Future> requests = new ConcurrentHashMap<MessageId, Future>();
    final private AtomicInteger sequence = new AtomicInteger();
    private ExecutorService executor;

    public HiTab() {
    }

    @Override
    public void init() throws Exception{
        timer = getTransport().getTimer();
        buffer = new HiTabBuffer(this, ackWait);
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
                HiTabHeader header = (HiTabHeader) message.getHeader(this.id);
                if (message.isFlagSet(Message.Flag.OOB) && header == null)
                    return up_prot.up(event);
                else if (header == null)
                    return up_prot.up(event);

                switch (header.getType()) {
                    case HiTabHeader.EMPTY_ACK_MESSAGE:
                        buffer.addPlaceholders(header.getAckInformer(), header.getAckList());
                        break;
                    case HiTabHeader.RETRANSMISSION:
                        Future request = requests.get(header.getId());
                        if (request != null)
                            request.cancel(false);

                        if (message.getSrc().equals(localAddress) || messageStore.containsKey(header.getId()))
                            break;
                    case HiTabHeader.BROADCAST:
                        messageStore.put(header.getId(), message);
                        buffer.addMessage(message, view);
                        break;
                    case HiTabHeader.PLACEHOLDER_REQUEST:
                        if (message.getSrc().equals(localAddress))
                            break;
                        handlePlaceholderRequest(header);
                        break;
                    case HiTabHeader.SEQUENCE_REQUEST:
                        if (message.getSrc().equals(localAddress))
                            break;
                        handleSequenceRequest(header);
                        break;
                }
                // Return null so that the up event only occurrs if a message has been delivered from the buffer
                // or its not HiTab message or is OOB
                return null;
            case Event.VIEW_CHANGE:
                view = (View) event.getArg();
                break;
            case Event.USER_DEFINED:
                HiTabEvent e = (HiTabEvent) event.getArg();
                switch (e.getType()) {
                    case HiTabEvent.ACK_MESSAGE:
                        ackMessage((MessageId) e.getArg());
                        break;
                }
                return null;
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
        if (!requestInProgress(header.getId())) {
            requestStatus.put(header.getId(), System.nanoTime());
            sendRequest(header);
        }
    }

    public long getCurrentTime() {
        return (Long) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_TIME)));
    }

    public NMCData getNMCData() {
        return (NMCData) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES)));
    }

    private void deliverMessage(Message message) {
//        System.out.println("^^^^ Deliver | " + ((HiTabHeader)message.getHeader((short)1004)).getId());
        up_prot.up(new Event(Event.MSG, message));
    }

    private void sendRequest(HiTabHeader header) {
        Message message = new Message();
        message.putHeader(this.id, header)
               .setFlag(Message.Flag.DONT_BUNDLE)
               .setFlag(Message.Flag.OOB);
        down_prot.down(new Event(Event.MSG, message));
    }

    private void sendMessage(Message message) {
        if (sendBlankMessage != null)
            sendBlankMessage.cancel(false);
        NMCData data = getNMCData();
        MessageId id;
        synchronized (sequence) {
            id = new MessageId(getCurrentTime(), localAddress, sequence.getAndIncrement());
        }
        HiTabHeader header = new HiTabHeader(id, HiTabHeader.BROADCAST, data.getCapD(), data.getCapS(), data.getXMax(), localAddress, getMessageAcks());
        message.putHeader(this.id, header)
               .setFlag(Message.Flag.DONT_BUNDLE);
        ackTimeout();
    }

    private void sendBlankMessage() {
        if (acks.size() > 0) {
            MessageId id = new MessageId(-1, localAddress, -1);
            HiTabHeader header = HiTabHeader.createAckInformer(id, localAddress, getMessageAcks());
            Message blankMessage = new Message(null, new byte[0]);
            blankMessage.putHeader(this.id, header)
                    .setFlag(Message.Flag.DONT_BUNDLE);
            down_prot.down(new Event(Event.MSG, blankMessage));
        }
        ackTimeout();
    }

    private void ackTimeout() {
        sendBlankMessage = timer.schedule(new Runnable() {
            @Override
            public void run() {
                sendBlankMessage();
            }
        }, ackWait, TimeUnit.MILLISECONDS);
    }

    private void resendMessage(MessageId id) {
        Message message = messageStore.get(id);
        // setDest(null) Necessary to ensure that the retransmission is broadcast to all nodes
        // Mainly useful for testing that the placeholder request is working
        // i.e. we can send a message to one destination and because of the acks this message will be considered lost by other nodes
        if (message.getDest() != null)
            message.setDest(null);
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
        if (requestedMessage == null) {
            System.out.println("Requested Message == null | " + id);
            return;
        }
        // Reload the actual id, so that the equals method will correctly return true when searching our records
        // This is necessary because the sequence request will be missing the originator field
        // Therefore the equals method will return false if the above id is used, even if we have the message
        id = ((HiTabHeader)requestedMessage.getHeader(this.id)).getId();
        if (validRequest(id)) {
//            System.out.println("Valid Sequence Request | id := " + id);
//            requestStatus.put(id, System.nanoTime());
            if (id.getOriginator().equals(localAddress))
                resendMessage(id);
            else
                requestTimeout(id, requestedMessage);
        }
    }

    private void handlePlaceholderRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage = messageStore.get(id);
        if (requestedMessage == null) {
            buffer.addPlaceholder(header.getAckInformer(), id);
            return;
        }

        if (validRequest(id)) {
            requestStatus.put(id, System.nanoTime());
            if (header.getAckInformer().equals(localAddress)) {
                resendMessage(id);
            } else {
                requestTimeout(id, requestedMessage);
            }
        }
    }

    private boolean validRequest(MessageId id) {
        if (requestInProgress(id))
            return false;

        boolean flag = (Boolean) down_prot.down(new Event(Event.USER_DEFINED,
                new HiTabEvent(HiTabEvent.BROADCAST_COMPLETE, id)));

        if (!flag)
            System.out.println("Broadcast Not Complete");
        return flag;
    }

    public Message getMessageBySequence(MessageId id) {
        for (MessageId msgId : messageStore.keySet()) {
            if (id.getSequence() == msgId.getSequence() && id.getOriginator().equals(msgId.getOriginator()))
                return messageStore.get(msgId);
        }
        return null;
    }

    // Returns true if a request status cannot be found, as this means that the request has been satisfied!
    private boolean requestInProgress(MessageId id) {
        Long lastRequest = requestStatus.get(id);
        if (lastRequest == null)
            return false;

        long timeSinceLast = System.nanoTime() - lastRequest;
        return TimeUnit.MILLISECONDS.convert(timeSinceLast, TimeUnit.NANOSECONDS) < requestTimeout;
    }


    private void ackMessage(MessageId id) {
        // If timestamp == -1 then the message was a EMPTY_ACK_MESSAGE and should not be acknowledged
        if (id.getTimestamp() == -1)
            return;

        if (!acks.contains(id)) {
            try {
                // Prevents deadlock as .put() blocks
                // This becomes a problem as sendBlankMessage() and ackTimeout() utilise the same timer threads as
                // The disseminator tasks.  Therefore with put() it is possible for all running threads to block,
                // preventing the ackTimeout() task from ever executing. (Because a new thread will not be created)
                while (acks.offer(id, 500000, TimeUnit.NANOSECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();  // TODO: Customise this generated block
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

    private void requestTimeout(final MessageId id, final Message message) {
        final NMCData data = getNMCData();
        int delay = (int) Math.ceil(data.getOmega() + data.getXMax());
        Future f = timer.schedule(new Runnable() {
            @Override
            public void run() {
//                final MessageId id = ((HiTabHeader) message.getHeader(HiTab.this.id)).getId();
                if (!requestInProgress(id)) {
                    Random r = new Random();
                    int delay = r.nextInt((int) Math.ceil(data.getEta()));
                    Future f = timer.schedule(new Runnable() {
                        @Override
                        public void run() {
                            if (!requestInProgress(id))
                                resendMessage(message);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                    requests.put(id, f);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
        requests.put(id, f);
    }

    public int getRequestTimeout() {
        return requestTimeout;
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