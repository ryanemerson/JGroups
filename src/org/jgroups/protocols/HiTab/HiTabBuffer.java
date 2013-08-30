package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class HiTabBuffer {

    final private HiTab hitab;
    final private LinkedList<MessageRecord> buffer; // Stores the message record
    final private Map<Address, Long> sequenceRecord; // Stores the largest delivered sequence for each known node
    final private int ackWait;
    final private long maxError; // The maximum error rate of the probabilistic clock synch
    private MessageRecord lastDeliveredMessage; // The timestamp of the last message that was delivered;
    private View view; // The latest view of the cluster

    public HiTabBuffer(HiTab hitab, int ackWait) {
        this.hitab = hitab;
        this.ackWait = ackWait;
        this.buffer = new LinkedList<MessageRecord>();
        this.sequenceRecord = new ConcurrentHashMap<Address, Long>();
        this.maxError = (Integer) hitab.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_ERROR)));
        this.lastDeliveredMessage = null;
    }

    public boolean addMessage(Message message, View view) {
        this.view = view;
        updateSequences(view);

        MessageRecord record = new MessageRecord(message);
        synchronized (buffer) {
            if (oldSequence(record))
                return false;

            calculateDeliveryTime(record);
            if (validMsgTime(record)) {
                addPlaceholders(record);
                addMessage(record);
                return true;
            } else {
                System.err.println("Message REJECTED as it has arrived too late");
                System.err.println("Rejected Message := " + record.id);
                System.err.println("Last Delivered := " + lastDeliveredMessage.id);

                // Increment expected sequence number to stop subsequent messages from blocking
                updateSequence(record, record.id.getOriginator());
                return false;
            }
        }
    }

    public List<Message> process() throws InterruptedException {
        List<Message> deliverable = new ArrayList<Message>();
        synchronized (buffer) {
            while (buffer.isEmpty()) {
                buffer.wait();
            }

            MessageRecord record = buffer.getFirst();
            if (record.placeholder) {
                System.out.println("SEND PLACEHOLDER REQUEST | " + record.id);
                hitab.sendPlaceholderRequest(record.id, record.ackInformer);
                buffer.wait();
            }

            long expectedSequence = getSequence(record.id.getOriginator());
            if (record.id.getSequence() > expectedSequence) {
                System.out.println("SEND SEQUENCE REQUEST - Expected Sequence := " + expectedSequence + " | Actual Sequence := " + record.id.getSequence() + record);
                hitab.sendSequenceRequest(record.id.getOriginator(), expectedSequence);
                buffer.wait();
            }

            Iterator<MessageRecord> i = buffer.iterator();
            while (i.hasNext()) {
                record = i.next();
                if(!record.placeholder && record.deliveryTime <= hitab.getCurrentTime()) {
                    expectedSequence = getSequence(record.id.getOriginator());
                    if (record.id.getSequence() == expectedSequence) {
                        updateSequence(record, record.id.getOriginator());
                        deliverable.add(record.message);
                        i.remove();
                    }
                } else {
                    break;
                }
                lastDeliveredMessage = record;
            }
            return deliverable;
        }
    }

    private void calculateDeliveryTime(MessageRecord record) {
        NMCData data = hitab.getNMCData();
        HiTabHeader header = record.getHeader();
        int timeout1 = (int) Math.ceil(data.getEta() + data.getOmega());
        int x = (int) Math.ceil(data.getOmega() + (2 * data.getEta()));
        int latencyPeriod = Math.max(header.getCapD(), header.getXMax() + header.getCapS());

        int delay = timeout1 + header.getXMax() + x + ackWait + latencyPeriod;
        long delayNano = delay * 1000000L;

        long initialC = header.getId().getTimestamp() + delayNano + maxError;
        long receivedTime = hitab.getCurrentTime();

        long deliveryDelay = initialC - receivedTime;
        record.deliveryTime = receivedTime + deliveryDelay;
    }

    private void addMessage(MessageRecord record) {
        // Add the message to the message store
        if (buffer.isEmpty()) {
            buffer.add(record);
            buffer.notify();
            return;
        }
        // If a placeholder already exists for this message then simply replace it with the actual message
        int placeholderIndex = buffer.indexOf(record);
        if (placeholderIndex > -1) {
            buffer.set(placeholderIndex, record);
            buffer.notify();
            return;
        }

        // If message was created before the oldest message in the buffer then add to the start of the buffer
        if (record.id.getTimestamp() < buffer.getFirst().id.getTimestamp()) {
            for (MessageRecord newerRecord : buffer) {
                if (!newerRecord.placeholder) {
                    if (record.deliveryTime > newerRecord.deliveryTime) {
                        newerRecord.deliveryTime = record.deliveryTime;
                    } else {
                        break;
                    }
                }
            }
            buffer.add(0, record);
            buffer.notify();
        }
        // If this message was created before all other messages add to the end of the buffer
        else if (record.id.getTimestamp() > buffer.getLast().id.getTimestamp()) {
            // Get the lastMessageDelay in the buffer that is not equal to -1 (placeholder) if no other messages use
            // original deliver delay
            long lastMessageDelay = -1;
            ListIterator<MessageRecord> li = buffer.listIterator();
            while (li.hasPrevious()) {
                lastMessageDelay = li.previous().deliveryTime;
                if (lastMessageDelay != -1) {
                    break;
                }
            }
            if (lastMessageDelay != -1 && lastMessageDelay > record.deliveryTime) {
                record.deliveryTime = lastMessageDelay;
            }
            buffer.add(buffer.size(), record);
            buffer.notify();
        }
        // Otherwise find its appropriate place in the buffer and update delivery times where necessary
        else {
            MessageRecord previous = null;
            MessageRecord next = null;

            for (int i = 0; i < buffer.size(); i++) {
                int index = -1;
                previous = next;
                next = buffer.get(i);

                if (record.id.getTimestamp() == next.id.getTimestamp()
                        || (previous != null && record.id.getTimestamp() > previous.id.getTimestamp()
                        && record.id.getTimestamp() < next.id.getTimestamp())) {

                    index = i;
                    // If previous record is a placeholder then deliveryTime will always be less than this message
                    if (previous != null && previous.deliveryTime > record.deliveryTime)
                        record.deliveryTime = previous.deliveryTime;

                    // If the times are original timestamp is the same for two messages, tie-break
                    // Preference is given to the node which has the lowest index in the view
                    if (record.id.getTimestamp() == next.id.getTimestamp())
                        if (view.getMembers().indexOf(record.id.getOriginator()) > view.getMembers().indexOf(next.id.getOriginator()))
                            index++;

                    for (int j = i; j < buffer.size(); j++) {
                        MessageRecord futureMessage = buffer.get(j);
                        if (record.deliveryTime > futureMessage.deliveryTime) {
                            if (!futureMessage.placeholder) {
                                futureMessage.deliveryTime = record.deliveryTime;
                            }
                        } else {
                            break;
                        }
                    }
                    buffer.add(index, record);
                    buffer.notify();
                    break;
                }
            }
        }
    }

    public void addPlaceholders(Address ackInformer, List<MessageId> ackList) {
        if (ackList.size() < 1) {
            return;
        }
        for (MessageId id : ackList) {
            addPlaceholder(ackInformer, id);
        }
    }

    private void addPlaceholders(MessageRecord record) {
        addPlaceholders(record.ackInformer, record.getHeader().getAckList());
    }


    public void addPlaceholder(Address ackInformer, MessageId id) {
        if (oldSequence(id))
            return;

        synchronized (buffer) {
            boolean messageReceived;
            MessageRecord placeholder = new MessageRecord(ackInformer, id);
            if (oldSequence(placeholder) || !validMsgTime(placeholder) || buffer.contains(placeholder)) {
                return;
            }

            if (buffer.isEmpty()) {
                buffer.add(placeholder);
                return;
            }

            if (placeholder.id.getTimestamp() < buffer.getFirst().id.getTimestamp()) {
                buffer.add(0, placeholder);
            } else if (placeholder.id.getTimestamp() > buffer.getLast().id.getTimestamp()) {
                buffer.add(buffer.size(), placeholder);
            } else {
                MessageRecord previous;
                MessageRecord next = null;
                for (int i = 0; i < buffer.size(); i++) {
                    int index = i;
                    previous = next;
                    next = buffer.get(i);

                    // If the times are original timestamp is the same for two messages, tie-break
                    // Preference is given to the node which has the lowest index in the view
                    if (id.getTimestamp() == next.id.getTimestamp()) {
                        if (view.getMembers().indexOf(id.getOriginator()) > view.getMembers().indexOf(next.id.getOriginator())) {
                            index = i;
                            index++;
                        }
                        buffer.add(index, placeholder);
                        break;
                    }
                    if (previous != null && placeholder.id.getTimestamp() > previous.id.getTimestamp()
                            && placeholder.id.getTimestamp() < next.id.getTimestamp()) {
                        index = i;
                        buffer.add(index, placeholder);
                        break;
                    }
                }
            }
        }
    }

    public long getSequence(Address origin) {
        return sequenceRecord.get(origin);
    }

    private void updateSequence(MessageRecord record, Address origin) {
        synchronized (sequenceRecord) {
            sequenceRecord.put(origin, sequenceRecord.get(origin) + 1);
        }
    }

    private void updateSequences(View view) {
        for (Address address : view.getMembers()) {
            synchronized (sequenceRecord) {
                if (!sequenceRecord.containsKey(address))
                    sequenceRecord.put(address, 0L);
            }
        }
    }

    private boolean validMsgTime(MessageRecord record) {
        if (lastDeliveredMessage == null) {
            return true;
        }
        return record.id.getTimestamp() >= lastDeliveredMessage.id.getTimestamp();
    }

    private boolean oldSequence(MessageId id) {
        long expectedSequence = getSequence(id.getOriginator());
        return id.getSequence() < expectedSequence;
    }

    private boolean oldSequence(MessageRecord record) {
        return oldSequence(record.id);
    }

    final class MessageRecord {
        final private MessageId id;
        private Message message;
        private Address ackInformer;
        private boolean placeholder;
        private boolean readyToDeliver;
        private long deliveryTime;

        MessageRecord(MessageId id, Message message, Address ackInformer, boolean placeholder, boolean readyToDeliver, long deliveryTime) {
            this.id = id;
            this.message = message;
            this.ackInformer = ackInformer;
            this.placeholder = placeholder;
            this.readyToDeliver = readyToDeliver;
            this.deliveryTime = deliveryTime;
        }

        MessageRecord(Message message) {
            HiTabHeader header = (HiTabHeader) message.getHeader(ClassConfigurator.getProtocolId(HiTab.class));
            this.id = header.getId();
            this.message = message;
            this.ackInformer = header.getAckInformer();
            this.placeholder = false;
            this.readyToDeliver = false;
            this.deliveryTime = -1;
        }

        // Placeholder constructor
        MessageRecord(Address ackInformer, MessageId id) {
            this.id = id;
            this.message = null;
            this.ackInformer = ackInformer;
            this.placeholder = true;
            this.readyToDeliver = false;
            this.deliveryTime = -1;
        }

        HiTabHeader getHeader() {
            return (HiTabHeader) message.getHeader(ClassConfigurator.getProtocolId(HiTab.class));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (id != null ? !id.equals(that.id) : that.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", message=" + message +
                    ", ackInformer=" + ackInformer +
                    ", placeholder=" + placeholder +
                    ", readyToDeliver=" + readyToDeliver +
                    ", deliveryTime=" + deliveryTime +
                    "} " + super.toString();
        }
    }
}