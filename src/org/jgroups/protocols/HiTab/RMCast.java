package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    @Property(name="max_piggybacked_headers", description="The maximum number of RMCastHeaders that can be piggybacked onto a message")
    private int maxHeaders = 1;

    @Property(name="ip_multicast", description="If true broadcasts are ip_multicast, otherwise they are sent as unicasts")
    private boolean multicast = true;

    private AtomicInteger numberOfNormalMsgs = new AtomicInteger();
    private AtomicInteger numberOfDissMsgs = new AtomicInteger();
    private AtomicInteger numberOfExplicitCopies = new AtomicInteger();
    private AtomicInteger numberOfMessageRequests = new AtomicInteger();

    private final Map<MessageId, MessageRecord> messageRecords = new ConcurrentHashMap<MessageId, MessageRecord>();
    private final Map<MessageId, Message> receivedMessages = new ConcurrentHashMap<MessageId, Message>();
    private final Map<RMCastHeader, Future> messageCopyTasks = new ConcurrentHashMap<RMCastHeader, Future>();
    private final Map<RMCastHeader, MessageBroadcaster> messageCopyBroadcaster = new ConcurrentHashMap<RMCastHeader, MessageBroadcaster>();
    private final Queue<RMCastHeader> messageCopyQueue = new ConcurrentLinkedQueue<RMCastHeader>();
    private final Map<MessageId, Future> responsiveTasks = new ConcurrentHashMap<MessageId, Future>();
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
        System.out.println("#RMCAST Norm := " + numberOfNormalMsgs.intValue());
        System.out.println("#RMCAST Diss := " + numberOfDissMsgs.intValue());
        System.out.println("#RMCAST Explicit Copies := " + numberOfExplicitCopies.intValue());
        System.out.println("#RMCAST Copy requests := " + numberOfMessageRequests.intValue());
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
                if (type == HiTabHeader.BROADCAST || type == HiTabHeader.RETRANSMISSION) {
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
                            complete = record.isBroadcastComplete();
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
//        System.out.println("--------------------------------------------------------------");
//        System.out.println("Received := " + header);

        final MessageRecord record;
        MessageRecord newRecord = new MessageRecord(header);
        MessageRecord tmp = (MessageRecord) ((ConcurrentHashMap)messageRecords).putIfAbsent(header.getId(), newRecord);
        if (tmp == null) {
            up_prot.up(event); // Deliver to the above layer (Application or HiTab) if this is the first time RMCast has received M
            record = newRecord;
            receivedMessages.put(header.getId(), (Message) event.getArg()); // Store actual message, need for retransmission
            handlePiggyBacks(header);
        } else {
            if (((HiTabHeader)header).getType() == HiTabHeader.RETRANSMISSION)
                up_prot.up(event);
            record = tmp;
        }
        handleRMCastCopies(header, record);
    }

    private void handleRMCastCopies(RMCastHeader header, MessageRecord record) {
        if (!record.ackNotified && header.getCopy() > 0) {
            record.ackNotified = true;
            up_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.ACK_MESSAGE, record.id)));
        }

        if (record.largestCopyReceived == header.getCopyTotal() && record.crashNotified) {
            // Cancel any responsiveness tasks that belong to this message and remove the record
            Future f = responsiveTasks.get(header.getId());
            if (f != null)
                f.cancel(true);
            receivedMessages.remove(header.getId());
            return;
        }

        if (header.getDisseminator().equals(header.getId().getOriginator()) && !record.crashNotified) {
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
                case HiTabHeader.BROADCAST:
                    totalCopies = data.getMessageCopies();
                    existingHeader.setCopyTotal(totalCopies);
                    existingHeader.setDisseminator(localAddress);
                    getHeadersToPiggyback(existingHeader); // Add piggybacked headers
                    initialCopy = 0;
                    break;
                case HiTabHeader.RETRANSMISSION:
                    existingHeader.setDisseminator(localAddress);
                    initialCopy = totalCopies = existingHeader.getCopyTotal();
                    break;
                case HiTabHeader.EMPTY_ACK_MESSAGE:
                case HiTabHeader.PLACEHOLDER_REQUEST:
                    initialCopy = totalCopies = 0;
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

            final RMCastHeader header = new RMCastHeader(msgId, localAddress, initialCopy, data.getMessageCopies());
            getHeadersToPiggyback(header); // Add piggybacked headers
            message.putHeader(this.id, header);
        }
        timer.execute(new MessageBroadcaster(message, initialCopy, totalCopies, (int) data.getEta(), headerId));
    }

    private void getHeadersToPiggyback(RMCastHeader header) {
        List<RMCastHeader> headers = new ArrayList<RMCastHeader>();
        while (headers.size() < maxHeaders) {
            RMCastHeader h = messageCopyQueue.poll();
            if (h == null)
                break;

            if (messageCopyTasks.get(h).cancel(false)) {
                h.setPiggyBackedHeaders(null);
                headers.add(h);

                // If more copies of this message need to be sent
                // Create a new MessageBroadcaster task, schedule it and add its details to the appropriate
                // Data structures so that it can be piggybacked as well
                if (h.getCopy() < h.getCopyTotal()) {
                    MessageBroadcaster newMb = messageCopyBroadcaster.get(h).nextCopy();
                    Future f = timer.schedule(newMb, newMb.delay, TimeUnit.MILLISECONDS);

                    // Create new ref so that the futures associated with h can be removed after the if statement
                    // The headers copy number has changed so newHeader is not equal to h
                    RMCastHeader newHeader = h;
                    newHeader.setCopy(newMb.currentCopy.intValue());
                    messageCopyTasks.put(newHeader, f);
                    messageCopyQueue.add(newHeader);
                    messageCopyBroadcaster.put(newHeader, newMb);
                }
                removeOldHeaderData(h);
            }
        }
        header.setPiggyBackedHeaders(headers);
    }

    private void handlePiggyBacks(RMCastHeader header) {
        List<RMCastHeader> headers = header.getPiggyBackedHeaders();
        if (headers != null) {
            for (RMCastHeader h : headers) {
                if (!messageRecords.containsKey(h.getId())) {
                    requestFullMessage(h.getId());
                } else {
                    MessageRecord record = messageRecords.get(header.getId());
                    if (record.largestCopyReceived < h.getCopy())
                        record.largestCopyReceived = h.getCopy(); // Set the largest copy to == this piggybacks copy
                    handleRMCastCopies(header, record);
                }
            }
        }
    }

    private void requestFullMessage(MessageId id) {
        numberOfMessageRequests.incrementAndGet();
        up_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.MISSING_MESSAGE, id)));
    }

    private void responsivenessTimeout(final MessageRecord record, final RMCastHeader header) {
        final NMCData data = getNMCData();
        record.largestCopyReceived = header.getCopy();
        record.broadcastLeader = header.getDisseminator();
        // Set Timeout 2 for n + w
        final int timeout = (int) Math.ceil(data.getEta() + data.getOmega());
        // If there is already a responsiveness timeout in progress (Executing), do nothing
        // Otherwise cancel the timeout and start a new one
        Future oldTask = responsiveTasks.get(record.id);
        if (oldTask == null || oldTask.cancel(false)) {
            // Final check before creating the task ensuring another thread hasn't completed the broadcast of this message
            if (record.largestCopyReceived >= header.getCopyTotal())
                return;
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
                                // Check to see if we still need to start disseminating
                                if (record.largestCopyReceived < header.getCopyTotal()) {
                                    record.broadcastLeader = localAddress;
                                    timer.execute(new MessageDisseminator(record, header, (int) data.getEta()));
                                }
                            }
                        }, ran, TimeUnit.MILLISECONDS);
                        // Set the responsivenes task to be this newTask.
                        // Allows this newTask to be cancelled if this method is called by a subsequent message copy
                        responsiveTasks.put(record.id, nextTask);
                    }
                }
            }, timeout, TimeUnit.MILLISECONDS);
            // Store this task so that it can be used by the above if statement when the method is called again
            responsiveTasks.put(record.id, task);
        }
    }

    private void  collectGarbage(List<MessageId> messages) {
        for (MessageId id : messages) {
            messageRecords.remove(id);
            receivedMessages.remove(id); // Shouldn't be necessary
            Future f = responsiveTasks.remove(id);
            if (f != null)
                f.cancel(true);
        }
    }

    private void removeOldHeaderData(RMCastHeader header) {
        messageCopyQueue.remove(header);
        messageCopyTasks.remove(header);
        messageCopyBroadcaster.remove(header);
    }

    final class MessageBroadcaster implements Runnable {
        private final Message message;
        private final int totalCopies;
        private final int delay;
        private final short headerId;
        private final AtomicInteger currentCopy;

        public MessageBroadcaster nextCopy() {
            return new MessageBroadcaster(message, currentCopy.incrementAndGet(), totalCopies, delay, headerId);
        }

        public MessageBroadcaster(Message message, int currentCopy, int totalCopies, int delay, short headerId) {
            this.message = message;
            this.currentCopy = new AtomicInteger(currentCopy);
            this.totalCopies = totalCopies;
            this.delay = delay;
            this.headerId = headerId;
        }

        @Override
        public void run() {
            ((RMCastHeader) message.getHeader(headerId)).setCopy(currentCopy.intValue());
//            down_prot.down(new Event(Event.MSG, message));
            broadcastMessage(message);

            numberOfNormalMsgs.incrementAndGet();
            if (currentCopy.intValue() > 0)
                numberOfExplicitCopies.incrementAndGet();

            executeAgain();
        }

        // We use this instead of timer.scheduleWithDynamicInterval because using timer.execute() executes the initial task
        // 6 times faster than using DynamicInterval.  Thus the first message copy is sent down the stack within a sixth
        // of the time it takes with DynamicInterval.
        private void executeAgain() {
            RMCastHeader header = (RMCastHeader) message.getHeader(headerId);
            removeOldHeaderData(header);
            if (header.getCopy() < header.getCopyTotal() && canBePiggyBacked(header)) {
                MessageBroadcaster mb = nextCopy();
                Future f = timer.schedule(mb, delay, TimeUnit.MILLISECONDS);
                // Need to add one to the header so that the equals method of the header functions correct
                header.setCopy(mb.currentCopy.intValue());
                messageCopyQueue.add(header);
                messageCopyTasks.put(header, f);
                messageCopyBroadcaster.put(header, mb);
            }
        }
    }

    private void broadcastMessage(Message message) {
        if (message.getDest() == null && !multicast) {
            for (Address address : view.getMembers()) {
                if (!address.equals(localAddress)) {
                    message.setDest(address);
                    down_prot.down(new Event(Event.MSG, message));
                }
            }
            message.setDest(localAddress);
            down_prot.down(new Event(Event.MSG, message)); // Send to self
        } else {
            down_prot.down(new Event(Event.MSG, message));
        }
    }

    private boolean canBePiggyBacked(RMCastHeader header) {
        return (isHiTabHeader(header) && ((HiTabHeader) header).getType() == HiTabHeader.BROADCAST);
    }

    private boolean isHiTabHeader(RMCastHeader header) {

        return header == null || header instanceof HiTabHeader;
    }

    final class MessageDisseminator implements Runnable {
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
            if (record.largestCopyReceived >= header.getCopyTotal() || message == null)
                return;

            int messageCopy = Math.max(record.lastBroadcast + 1, record.largestCopyReceived);
            header.setDisseminator(localAddress);
            header.setCopy(messageCopy);

            short protocolId = ClassConfigurator.getProtocolId(HiTab.class);
            RMCastHeader existingHeader = (RMCastHeader) message.getHeader(protocolId);
            if (existingHeader != null)
                message.putHeader(protocolId, header);
            else
                message.putHeader(id, header);

//            down_prot.down(new Event(Event.MSG, message));
            broadcastMessage(message);
            numberOfDissMsgs.incrementAndGet();
            // Update to show that the largestCopy received == last broadcast i.e we're the disseminator
            record.largestCopyReceived = messageCopy;
            record.lastBroadcast = messageCopy;
            if (!record.ackNotified) {
                record.ackNotified = true;
                up_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.ACK_MESSAGE, record.id)));
            }
            executeAgain();
        }

        // We use this instead of timer.scheduleWithDynamicInterval because using timer.execute() executes the initial task
        // 6 times faster than using DynamicInterval.
        public void executeAgain() {
            if (record.largestCopyReceived < header.getCopyTotal() && record.broadcastLeader.equals(localAddress))
                timer.schedule(new MessageDisseminator(record, header, delay), delay, TimeUnit.MILLISECONDS);
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

        public boolean isBroadcastComplete() {
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