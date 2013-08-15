package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private HiTabBuffer buffer;
    private Address localAddress = null;
    private TimeScheduler timer;
    private View view;

    private long sequence;
    private Map<MessageId, Message> messageStore;
    private Map<MessageId, Boolean> requestStatus;
    private ExecutorService executor;

    public HiTab() {
    }

    @Override
    public void init() throws Exception{
        System.out.println("Init");
        timer = getTransport().getTimer();
        buffer = new HiTabBuffer(this, ackWait);
        requestStatus = new ConcurrentHashMap<MessageId, Boolean>();
        messageStore = new ConcurrentHashMap<MessageId, Message>();
    }

    @Override
    public void start() throws Exception {
        super.start();
        // Thread dedicated to delivering messages.  Required, so must be separate from the default thread pool
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());
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
                if (header == null)
                    return up_prot.up(event);

                System.out.println("HiTab RECEIVED:= " + HiTabHeader.type2String(header.getType()));
                switch (header.getType()) {
                    case HiTabHeader.RETRANSMISSION:
                        requestStatus.put(header.getId(), true);
                    case HiTabHeader.BROADCAST:
                        messageStore.put(header.getId(), message);
                        buffer.addMessage(message, view);
                        break;
                    case HiTabHeader.PLACEHOLDER_REQUEST:
                        handlePlaceholdeRequest(header);
                        break;
                    case HiTabHeader.SEQUENCE_REQUEST:
                        handleSequenceRequest(header);
                        break;
                }
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
                sendMessage(message);
                return down_prot.down(event);

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);
            default:
                return down_prot.down(event);
        }
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
        HiTabHeader header = new HiTabHeader(id, HiTabHeader.BROADCAST, data.getCapD(), data.getCapS(), data.getXMax(), localAddress, null);
        message.putHeader(this.id, header);
        message.setFlag(Message.Flag.DONT_BUNDLE);
    }

    // No need to reset flags as they should still be in place from the original broadcast
    private void resendMessage(MessageId id) {
        Message message = messageStore.get(id);
        HiTabHeader header = (HiTabHeader) message.getHeader(this.id);
        header.setType(HiTabHeader.RETRANSMISSION);
        down_prot.down(new Event(Event.MSG, message));
    }

    private void handleSequenceRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage = getMessageBySequence(id);
        if (requestedMessage == null)
            return;
        if (validRequest(id)) {
            requestStatus.put(id, false);
            if (id.getOriginator().equals(localAddress)) {
                resendMessage(id);
                requestStatus.remove(id);
            } else {
                timer.execute(new RequestTimeout(id));
            }
        }

    }

    private void handlePlaceholdeRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage = messageStore.get(id);
        if (requestedMessage == null) {
            buffer.addPlaceholder(id);
            return;
        }
        if (validRequest(id)) {
            requestStatus.put(id, false);
            if (header.getAckInformer().equals(localAddress)) {
                resendMessage(id);
                requestStatus.remove(id);
            } else {
                timer.execute(new RequestTimeout(id));
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
        if (requestStatus.get(id) == null) {
            return true;
        }
        return requestStatus.get(id);
    }

    final class RequestTimeout implements Runnable {
        private final MessageId id;
        RequestTimeout(MessageId id) {
            this.id = id;
        }

        @Override
        public void run() {
            NMCData data = getNMCData();
            try {
                int delay = (int) Math.ceil(data.getOmega() + data.getXMax());
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }

            if (requestStatus.get(id))
                requestStatus.remove(id);
            else {
                Random r = new Random();
                int delay = r.nextInt((int) Math.ceil(data.getEta()));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                }

                if (requestStatus.get(id))
                    requestStatus.remove(id);
                else {
                    resendMessage(id);
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
}