package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A protocol that handles the broadcasting of messages but provides no ordering
 *
 * @author ryan
 * @since 4.0
 */
public class RMCast extends Protocol {
    private final Map<MessageId, MessageRecord> messageRecords = new ConcurrentHashMap<MessageId, MessageRecord>();
    private final Map<MessageId, Message> receivedMessages = new ConcurrentHashMap<MessageId, Message>();
    private final Map<MessageId, Future> responsiveTasks = new ConcurrentHashMap<MessageId, Future>();
    private final Map<MessageId, Future> disseminatorTasks = new ConcurrentHashMap<MessageId, Future>();
    private Address localAddress = null;
    private TimeScheduler timer;
    private View view;

    @Override
    public void init() throws Exception {
        timer = getTransport().getTimer();
    }

    @Override
    public void start() throws Exception {
        System.out.println("START RMCAST!!!!!!");
        super.start();
    }

    @Override
    public void stop() {
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();

                if (message.isFlagSet(Message.Flag.OOB))
                    return up_prot.up(event);
                RMCastHeader header = (RMCastHeader) message.getHeader(this.id);
                if (header == null) {
                    header = (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(HiTab.class));
                    if (header == null)
                        return up_prot.up(event);
                }

                byte type = ((HiTabHeader) header).getType();
                if (type == HiTabHeader.BROADCAST || type == HiTabHeader.RETRANSMISSION || type == HiTabHeader.EMPTY_ACK_MESSAGE) {
                    receivedMessages.put(header.getId(), message); // Store actual message, need for retransmission
                    handleMessage(event, header);
                    return null;
                }
                // If its not a MessageBroadcaster, Retransmission or an Empty ack message, then it must be a request. Send to HiTab protocol
                return up_prot.up(event);
            case Event.VIEW_CHANGE:
                view = (View) event.getArg();
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                broadcastMessage(event);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);

            case Event.USER_DEFINED:
                HiTabEvent e = (HiTabEvent) event.getArg();
                switch (e.getType()) {
                    case HiTabEvent.BROADCAST_COMPLETE:
                        MessageId id = (MessageId) e.getArg();
                        MessageRecord record = messageRecords.get(id);
                        boolean complete = true;
                        if (record != null)
                           complete = record.broadcastComplete();
                        return complete;
                    case HiTabEvent.COLLECT_GARBAGE:
                        collectGarbage((List<MessageId>) e.getArg());
                    default:
                        return down_prot.down(event);
                }
            default:
                return down_prot.down(event);
        }
    }

    public NMCData getNMCData() {
        return (NMCData) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES)));
    }

    public int getNodeSeniority(Address node) {
        if (node == null) {
            return Integer.MAX_VALUE;
        }
        return view.getMembers().indexOf(node);
    }

    private void handleMessage(Event event, RMCastHeader header) {
        final MessageRecord record;
        MessageRecord newRecord = new MessageRecord(header);
        MessageRecord tmp = (MessageRecord) ((ConcurrentHashMap)messageRecords).putIfAbsent(header.getId(), newRecord);
        if (tmp == null) {
            up_prot.up(event); // Deliver to the above layer (Application or HiTab) if this is the first time RMCast has received M
            record = newRecord;
        } else {
            if (((HiTabHeader)header).getType() == HiTabHeader.RETRANSMISSION)
                up_prot.up(event);
            record = tmp;
        }

        if (!record.ackNotified && header.getCopy() > 0) {
            record.ackNotified = true;
            up_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.ACK_MESSAGE, record.id)));
        }

        if (record.largestCopyReceived == header.getCopyTotal() && record.crashNotified)
            return;

        if (header.getDisseminator() == header.getId().getOriginator() && !record.crashNotified) {
            // TODO SEND NO CRASH NOTIFICATION
            // Effectively cancels the CrashedTimeout as nothing will happen once this is set to true
            record.crashNotified = true;
        }

        if (header.getCopy() == header.getCopyTotal()) {
            record.largestCopyReceived = header.getCopy();
        } else {
            if (!header.getId().getOriginator().equals(localAddress)
                    && header.getCopy() > record.largestCopyReceived
                    || (header.getCopy() == record.largestCopyReceived && (header.getDisseminator().equals(header
                    .getId().getOriginator()) || getNodeSeniority(header.getDisseminator()) < getNodeSeniority(record.broadcastLeader)))) {
                responsivenessTimeout(record, header);
            }
        }
    }

    private void broadcastMessage(Event event) {
        final Message message = (Message) event.getArg();
        final NMCData data = getNMCData();
        final short headerId;
        final int initialCopy;
        final int totalCopies;

        short protocolId = ClassConfigurator.getProtocolId(HiTab.class);
        RMCastHeader existingHeader = (RMCastHeader) message.getHeader(protocolId);
        // If HiTab header is present, set the rmsys values in that header
        if (existingHeader != null) {
            headerId = protocolId;
            switch (((HiTabHeader) existingHeader).getType()) {
                case HiTabHeader.EMPTY_ACK_MESSAGE:
                case HiTabHeader.BROADCAST:
                    totalCopies = data.getMessageCopies();
                    existingHeader.setCopyTotal(totalCopies);
                    existingHeader.setDisseminator(localAddress);
                    initialCopy = 0;
                    break;
                case HiTabHeader.RETRANSMISSION:
                    existingHeader.setDisseminator(localAddress);
                    initialCopy = totalCopies = existingHeader.getCopyTotal();
                    break;
                default:
                    initialCopy = 0;
                    totalCopies = data.getMessageCopies();
            }
        } else {
            headerId = this.id;
            MessageId msgId = new MessageId(System.currentTimeMillis(), localAddress);
            initialCopy = 0;
            totalCopies = data.getMessageCopies();
            final RMCastHeader header = new RMCastHeader(msgId, localAddress,
                    initialCopy, data.getMessageCopies());
            message.putHeader(this.id, header);
        }
        // Using timer instead of Thread.sleep is MUCH faster!
        timer.scheduleWithDynamicInterval(new MessageBroadcaster(message, initialCopy, totalCopies, (int) data.getEta(), headerId));
    }

    private void cancelOldTask(MessageRecord record, Future task, Map<MessageId, Future> map) {
        Future oldTask = map.put(record.id, task);
        if (oldTask != null) {
            oldTask.cancel(false);
        }
    }

    private void responsivenessTimeout(final MessageRecord record, final RMCastHeader header) {
        final NMCData data = getNMCData();
        record.largestCopyReceived = header.getCopy();
        record.broadcastLeader = header.getDisseminator();

        // Set Timeout 2 for n + w
        final int timeout = (int) Math.ceil(data.getEta() + data.getOmega());
        Future task = timer.schedule(new Runnable() {
            @Override
            public void run() {
                if (record.largestCopyReceived < header.getCopyTotal()) {
                    record.broadcastLeader = null;
                    Random r = new Random();
                    int eta = (int) data.getEta();
                    int ran = r.nextInt(eta < 1 ? eta + 1 : eta);

                    Future nextTask = timer.schedule(new Runnable() {
                        public void run() {
                            record.broadcastLeader = localAddress;
                            Future oldFuture = disseminatorTasks.get(record.id);
                            if (oldFuture == null) {
                                Future dissTask = timer.scheduleWithDynamicInterval(new MessageDisseminator(record, header, (int) data.getEta()));
                                cancelOldTask(record, dissTask, disseminatorTasks);
                            }
                        }
                    }, ran, TimeUnit.MILLISECONDS);
                    cancelOldTask(record, nextTask, responsiveTasks);
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);
        cancelOldTask(record, task, responsiveTasks);
    }

    private void  collectGarbage(List<MessageId> messages) {
        synchronized (messageRecords) {
            for (MessageId message : messages) {
                messageRecords.remove(message);
                receivedMessages.remove(message);
                responsiveTasks.remove(message);
                disseminatorTasks.remove(message);
            }
        }
    }

    final class MessageBroadcaster implements Runnable, TimeScheduler.Task {
        private final Message message;
        private final int totalCopies;
        private final int delay;
        private final short headerId;
        private final AtomicInteger currentCopy;

        public MessageBroadcaster(Message message, int currentCopy, int totalCopies, int delay, short headerId) {
            this.message = message;
            this.currentCopy = new AtomicInteger(currentCopy);
            this.totalCopies = totalCopies;
            this.delay = delay;
            this.headerId = headerId;
        }

        @Override
        public void run() {
            ((RMCastHeader) message.getHeader(headerId)).setCopy(currentCopy.getAndIncrement());
            down_prot.down(new Event(Event.MSG, message));
        }

        @Override
        public long nextInterval() {
            if (currentCopy.intValue() > totalCopies)
                return 0;
            return delay;
        }
    }

    final class MessageDisseminator implements Runnable, TimeScheduler.Task {
        private final Message message;
        private final MessageRecord record;
        private final RMCastHeader header;
        private final int delay;

        public MessageDisseminator(MessageRecord record, RMCastHeader header, int delay) {
            this.record = record;
            this.header = header;
            this.delay = delay;
            this.message = receivedMessages.get(header.getId());
        }

        @Override
        public void run() {
            int messageCopy = Math.max(record.lastBroadcast + 1, record.largestCopyReceived);
            header.setDisseminator(localAddress);
            header.setCopy(messageCopy);

            short protocolId = ClassConfigurator.getProtocolId(HiTab.class);
            RMCastHeader existingHeader = (RMCastHeader) message.getHeader(protocolId);
            if (existingHeader != null)
                message.putHeader(protocolId, header);
            else
                message.putHeader(id, header);

            down_prot.down(new Event(Event.MSG, message));
            // Update to show that the largestCopy received == last broadcast i.e we're the disseminator
            record.largestCopyReceived = messageCopy;
            record.lastBroadcast = messageCopy;
            if (!record.ackNotified) {
                record.ackNotified = true;
                up_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.ACK_MESSAGE, record.id)));
            }
        }

        @Override
        public long nextInterval() {
            if (header == null) {
                System.out.println("HEADER IS NULL");
            } else if (record == null) {
                System.out.println("Record is null");
            }

            if (record.largestCopyReceived < header.getCopyTotal() && record.broadcastLeader.equals(localAddress))
                return delay;
            return -1;
        }
    }

    final class MessageRecord {
        final private MessageId id;
        final private int totalCopies;
        private volatile int largestCopyReceived; // Largest copy received
        private volatile Address broadcastLeader; // Set to null if none
        private volatile int lastBroadcast;
        private volatile boolean crashNotified; // Not currently used for anything
        private volatile boolean ackNotified;

        public MessageRecord(RMCastHeader header) {
            this(header.getId(), header.getCopyTotal(), -1, null, -1, false, false);
        }

        public MessageRecord(MessageId id, int totalCopies) {
            this(id, totalCopies, -1, null, -1, false, false);
        }

        public MessageRecord(MessageId id, int totalCopies, int largestCopyReceived, Address broadcastLeader,
                             int lastBroadcast, boolean crashNotified, boolean ackNotified) {
            if (id == null)
                throw new IllegalArgumentException("A message records id feel cannot be null");

            this.id = id;
            this.totalCopies = totalCopies;
            this.largestCopyReceived = largestCopyReceived;
            this.broadcastLeader = broadcastLeader;
            this.lastBroadcast = lastBroadcast;
            this.crashNotified = crashNotified;
            this.ackNotified = ackNotified;
        }

        public boolean broadcastComplete() {
            if (largestCopyReceived != totalCopies && lastBroadcast != totalCopies) {
                System.out.println(toString());
                System.out.println(receivedMessages.containsKey(id));
            }
            return largestCopyReceived == totalCopies || lastBroadcast == totalCopies;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (crashNotified != that.crashNotified) return false;
            if (largestCopyReceived != that.largestCopyReceived) return false;
            if (lastBroadcast != that.lastBroadcast) return false;
            if (totalCopies != that.totalCopies) return false;
            if (broadcastLeader != null ? !broadcastLeader.equals(that.broadcastLeader) : that.broadcastLeader != null)
                return false;
            if (!id.equals(that.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + totalCopies;
            result = 31 * result + largestCopyReceived;
            result = 31 * result + (broadcastLeader != null ? broadcastLeader.hashCode() : 0);
            result = 31 * result + lastBroadcast;
            result = 31 * result + (crashNotified ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", totalCopies=" + totalCopies +
                    ", largestCopyReceived=" + largestCopyReceived +
                    ", broadcastLeader=" + broadcastLeader +
                    ", lastBroadcast=" + lastBroadcast +
                    ", crashNotified=" + crashNotified +
                    '}';
        }
    }
}