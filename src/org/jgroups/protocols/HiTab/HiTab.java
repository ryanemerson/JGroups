package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class HiTab extends Protocol {

    private volatile Map<Address, MessageId> lastDelivered = new HashMap<Address, MessageId>();
    private volatile Map<Address, MessageId> lastDeleted = new HashMap<Address, MessageId>();
    private final AtomicInteger retransmissions = new AtomicInteger();
    public final AtomicInteger placeholderRequests = new AtomicInteger();
    public final AtomicInteger abortedMessages = new AtomicInteger();
    public final AtomicInteger rejectedMessages = new AtomicInteger();
    private final AtomicInteger missingMessageRequest = new AtomicInteger();
    private final AtomicInteger abortRequest = new AtomicInteger();

    @Property(name = "ack_wait", description = "How long, in milliseconds, the system waits to receive an acknowlegment "
            + "before delivering a message.  This value is equal to the minimum broadcast rate allowed by the system")
    private int ackWait = 2000;

    @Property(name = "number_of_old_messages", description = "The minimum number of old messages that should be stored by " +
            "HiTab.  A large value increases the chance that a lost message will be recoverable.  A smaller value " +
            "reduces the protocols memory consumption.")
    private int numberOfOldMessages = 200;

    @Property(name = "garbage_collection", description = "How often, in minutes, the system performs garbage collection of old messages")
    private int garbageCollectionRate = 10;

    @Property(name = "buffer_timeout", description = "The amount of time in milliseconds for the buffer to wait after sending a message request")
    private int bufferTimeout = 5;

    private HiTabBuffer buffer;
    public Address localAddress = null; // TODO CHANGE TO PRIVATE
    private TimeScheduler timer;
    private long maxError; // The maximum error rate of the probabilistic clock synch

    private View view;

    private volatile Future<?> sendBlankMessage = null;
    final private Map<MessageId, Message> messageStore = new ConcurrentHashMap<MessageId, Message>();
    final private BlockingQueue<MessageId> acks = new ArrayBlockingQueue<MessageId>(500, true);
    final private Set<MessageId> requestsInProgress = Collections.synchronizedSet(new HashSet<MessageId>());
    final private Map<MessageId, Future> requests = new ConcurrentHashMap<MessageId, Future>();
    final private AtomicInteger sequence = new AtomicInteger();
    final private Map<MessageId, Message> sentMessageStore = new ConcurrentHashMap<MessageId, Message>();

    private ExecutorService executor;

    public HiTab() {
    }

    @Override
    public void init() throws Exception{
        timer = getTransport().getTimer();
        buffer = new HiTabBuffer(this, ackWait);
        maxError = (Integer) down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_ERROR)));
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
        System.out.println("#Retransmissions Sent := " + retransmissions.intValue());
        System.out.println("#PlaceholderRequests Sent := " + placeholderRequests.intValue());
        System.out.println("#MissingMessageRequests Sent := " + missingMessageRequest.intValue());
        System.out.println("#AbortRequests Sent := " + abortRequest.intValue());
        System.out.println("#Aborted Messages := " + abortedMessages.intValue());
        System.out.println("#Rejected Messages := " + rejectedMessages.intValue());
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
                        requestsInProgress.remove(header.getId());
                        Future request = requests.get(header.getId());
                        if (request != null)
                            request.cancel(true);

                        if (messageStore.containsKey(header.getId()))
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
                    case HiTabEvent.MISSING_MESSAGE:
                        MessageId id = (MessageId) e.getArg();
                        // Add placeholder so that the buffer blocks until rho == 0 arrives
                        buffer.addPlaceholder(id.getOriginator(), id);
//                        System.out.println("SEND MISSING MESSAGE REQUEST | " + id);
                        sendPlaceholderRequest(id, id.getOriginator());
                        missingMessageRequest.incrementAndGet();
                        break;
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

    public long getCurrentTime() {
        return (Long) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_TIME)));
    }

    public NMCData getNMCData() {
        return (NMCData) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES)));
    }

    public void sendMessageUp(Event event) {
        up_prot.up(event);
    }

    public long calculateDeliveryDelay(NMCData data, HiTabHeader header) {
        // Return in milliseconds
        return TimeUnit.MILLISECONDS.convert(calculateDeliveryTime(data, header) - getCurrentTime(), TimeUnit.NANOSECONDS);
    }

    public long calculateDeliveryTime(NMCData data, HiTabHeader header) {
        int timeout1 = (int) Math.ceil(data.getEta() + data.getOmega());
        int x = (int) Math.ceil(data.getOmega() + (2 * data.getEta()));
        int latencyPeriod = Math.max(header.getCapD(), header.getXMax() + header.getCapS());

        int delay = timeout1 + header.getXMax() + x + ackWait + latencyPeriod;
        long delayNano = delay * 1000000L;

        return header.getId().getTimestamp() + delayNano + maxError;
    }

    private void deliverMessage(Message message) {
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
        final MessageId id;
        synchronized (sequence) {
            id = new MessageId(getCurrentTime(), localAddress, sequence.getAndIncrement());
        }

        HiTabHeader header = new HiTabHeader(id, HiTabHeader.BROADCAST, data.getCapD(),
                data.getCapS(), data.getXMax(), localAddress, getMessageAcks());
        message.putHeader(this.id, header)
                .setFlag(Message.Flag.DONT_BUNDLE);
        ackTimeout();
        sentMessageStore.put(id, message);
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

    public void sendAbortMessage(MessageId id) {
        if (messageStore.containsKey(id))
            return;
//        System.out.println("Send Abort Message | " + id);
        HiTabHeader header = HiTabHeader.createAbortMessage(id);
        sendRequest(header);
        abortRequest.incrementAndGet();
        // TODO throw exception so that the application knows the message has failed
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
//        System.out.println("Resend Message | " + header.getId());
        header.setType(HiTabHeader.RETRANSMISSION);
        down_prot.down(new Event(Event.MSG, message));
        retransmissions.incrementAndGet();
    }

    private void handlePlaceholderRequest(HiTabHeader header) {
        MessageId id = header.getId();
        Message requestedMessage;
        if (id.getOriginator().equals(localAddress))
            requestedMessage = sentMessageStore.get(id);
        else
            requestedMessage = messageStore.get(id);

        if (requestedMessage == null) {
//            System.out.println("Requested Message not found | " + id);
//            System.out.println("Last Delivered := " + lastDelivered + " | Last Deleted := " + lastDeleted);
            buffer.addPlaceholder(header.getAckInformer(), id);
            return;
        }

        if (validRequest(id)) {
//            System.out.println("Valid request | " + id);
            requestsInProgress.add(id);
            if (header.getAckInformer().equals(localAddress))
                resendMessage(id);
            else
                requestTimeout(id, requestedMessage);
        }
//        else {
//            System.out.println("Not a valid request | " + header);
//        }
    }

    private boolean validRequest(MessageId id) {
        if (requestsInProgress.contains(id))
            return false;

        return (Boolean) down_prot.down(new Event(Event.USER_DEFINED,
                new HiTabEvent(HiTabEvent.BROADCAST_COMPLETE, id)));
    }

    private void requestTimeout(final MessageId id, final Message message) {
        final NMCData data = getNMCData();
        int delay = (int) Math.ceil(data.getOmega() + data.getXMax());
        Future oldRequest = requests.get(id);
        if (oldRequest == null || oldRequest.isDone()) {
            Future f = timer.schedule(new Runnable() {
                @Override
                public void run() {
                    Random r = new Random();
                    int delay = r.nextInt((int) Math.ceil(data.getEta()));
                    Future f = timer.schedule(new Runnable() {
                        @Override
                        public void run() {
                            resendMessage(message);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                    requests.put(id, f);
                }
            }, delay, TimeUnit.MILLISECONDS);
            requests.put(id, f);
        }
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
                while (!acks.offer(id, 500000, TimeUnit.NANOSECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();  // TODO: Customise this generated block
            }
        }
    }

    private List<MessageId> getMessageAcks() {
        List<MessageId> ackList = new ArrayList<MessageId>();
        acks.drainTo(ackList, 20);
        return ackList;
    }

    public int getBufferTimeout() {
        return bufferTimeout;
    }

    private void removeOldMessages(List<MessageId> messages) {
        for (MessageId id : messages) {
            messageStore.remove(id);
            requestsInProgress.remove(id);
            requests.remove(id);
            sentMessageStore.remove(id);
            lastDeleted.put(id.getOriginator(), id);
        }
    }

    final class DeliverMessages implements Runnable {
        @Override
        public void run() {
            LinkedBlockingQueue<MessageId> deliveredMessages = new LinkedBlockingQueue<MessageId>();
            while (true) {
                try {
                    List<Message> messages = buffer.process();
                    for (Message message : messages) {
                        deliverMessage(message);
                        HiTabHeader h = (HiTabHeader) message.getHeader(id);
                        lastDelivered.put(h.getId().getOriginator(), h.getId());
                        deliveredMessages.add(((HiTabHeader) message.getHeader(id)).getId());
                    }

                    if (deliveredMessages.size() > numberOfOldMessages) {
                        List<MessageId> oldMessages = new ArrayList<MessageId>();
                        int messagesToRemove = deliveredMessages.size() - numberOfOldMessages;
                        deliveredMessages.drainTo(oldMessages, messagesToRemove);
                        removeOldMessages(oldMessages);
                        down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.COLLECT_GARBAGE, oldMessages)));
                    }

                    LinkedBlockingQueue<MessageId> abortedMessages = (LinkedBlockingQueue) buffer.getRejectedMessages();
                    if (abortedMessages.size() > 0) {
                        List<MessageId> abortList = new ArrayList<MessageId>();
                        abortedMessages.drainTo(abortList);
                        removeOldMessages(abortList);
                        down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.COLLECT_GARBAGE, abortList)));
                    }
                } catch (InterruptedException e) {
                    break;
                }
                Util.sleep(1);
            }
        }
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
                if (seqDifference > numberOfOldMessages && broadcastComplete) {
                    garbage.add(id);
                }
            }
        }
        removeOldMessages(garbage);
        if (garbage.size() > 0)
            down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.COLLECT_GARBAGE, garbage)));
    }
    final class GarbageCollection implements Runnable {
        @Override
        public void run() {
            collectGarbage();
        }
    }
}