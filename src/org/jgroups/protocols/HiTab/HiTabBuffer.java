package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class HiTabBuffer {

    final private ReentrantLock lock;
    final private Condition notEmpty;
    final private HiTab hitab;
    final private LinkedList<MessageRecord> buffer; // Stores the message record
    final private Map<Address, AtomicLong> sequenceRecord; // Stores the largest delivered sequence for each known node
    final private int ackWait;
    final private long maxError; // The maximum error rate of the probabilistic clock synch
    private volatile MessageRecord lastDeliveredMessage; // The timestamp of the last message that was delivered;
    private View view; // The latest view of the cluster

    final private Queue<MessageRecord> recordQueue; // Stores message records before they are processed
    final private AtomicLong sequenceBlock;
    private volatile boolean isBlocked;
    private volatile Address originBlock;

    public HiTabBuffer(HiTab hitab, int ackWait) {
        this.hitab = hitab;
        this.ackWait = ackWait;
        this.buffer = new LinkedList<MessageRecord>();
        this.recordQueue = new ConcurrentLinkedQueue<MessageRecord>();
        this.maxError = (Integer) hitab.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_ERROR)));
        this.lastDeliveredMessage = null;
        this.sequenceRecord = Collections.synchronizedMap(new HashMap<Address, AtomicLong>());
        this.isBlocked = false;
        this.sequenceBlock = new AtomicLong(-1);
        this.originBlock = null;

        // Needed to ensure threads are treated fairly, important for ensuring that messages aren't rejected
        // because their handling thread could not aquire the lock.
        this.lock = new ReentrantLock(true);
        this.notEmpty = lock.newCondition();
    }

    public void addMessage(Message message, View view) {
        this.view = view;
        updateSequences(view);

        MessageRecord record = new MessageRecord(message);
        if (oldSequence(record))
            return;

        calculateDeliveryTime(record);
        if (validMsgTime(record)) {
            addRecordToQueue(record);
        } else {
            // Update the expected sequence for this origin to be equal to the rejected message's seq + 1
            // Necessary to prevent process() from entering an infinite loop of sendSequenceRequests!
            sequenceRecord.get(record.id.getOriginator()).set(record.id.getSequence() + 1);
            processBlockedMessage(record);

            HiTabHeader h = (HiTabHeader) message.getHeader((short) 1004);
            long currentTime = (Long) hitab.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_CLOCK_TIME)));
            long timeTaken = currentTime - h.getId().getTimestamp();
            System.err.println("****    Message REJECTED as it has arrived too late | " + timeTaken);
            System.err.println("****    Rejected Message := " + record.id);
            System.err.println("****    Last Delivered   := " + lastDeliveredMessage.id);
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

    private void addRecordToQueue(MessageRecord record) {
        addPlaceholders(record);
        recordQueue.add(record);
    }

    public void addPlaceholder(Address ackInformer, MessageId id) {
        recordQueue.add(new MessageRecord(ackInformer, id));
    }

    public void addPlaceholders(Address ackInformer, List<MessageId> ackList) {
        if (ackList.size() < 1) {
            return;
        }
        for (MessageId id : ackList) {
            recordQueue.add(new MessageRecord(ackInformer, id));
        }
    }

    private void addPlaceholders(MessageRecord record) {
        addPlaceholders(record.ackInformer, record.getHeader().getAckList());
    }

    private void addPlaceholderToBuffer(MessageRecord placeholder) {
        if (oldSequence(placeholder) || !validMsgTime(placeholder) || buffer.contains(placeholder))
            return;

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
                if (placeholder.id.getTimestamp() == next.id.getTimestamp()) {
                    if (view.getMembers().indexOf(placeholder.id.getOriginator()) > view.getMembers().indexOf(next.id.getOriginator())) {
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

    private void addMessageToBuffer(MessageRecord record) {
        // Add the message to the message store
        if (buffer.isEmpty()) {
            buffer.add(record);
            return;
        }
        // If a placeholder already exists for this message then simply replace it with the actual message
        int placeholderIndex = buffer.indexOf(record);
        if (placeholderIndex > -1) {
            buffer.set(placeholderIndex, record);
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
        }
        // If this message was created before all other messages add to the end of the buffer
        else if (record.id.getTimestamp() > buffer.getLast().id.getTimestamp()) {
            // Get the lastMessageDelay in the buffer that is not equal to -1 (placeholder) if no other messages use
            // original deliver delay
            long lastMessageDelay = -1;
            ListIterator<MessageRecord> li = buffer.listIterator();
            while (li.hasPrevious()) {
                lastMessageDelay = li.previous().deliveryTime;
                if (lastMessageDelay != -1)
                    break;
            }
            if (lastMessageDelay != -1 && lastMessageDelay > record.deliveryTime)
                record.deliveryTime = lastMessageDelay;

            buffer.add(buffer.size(), record);
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
                            if (!futureMessage.placeholder)
                                futureMessage.deliveryTime = record.deliveryTime;
                        } else
                            break;
                    }
                    buffer.add(index, record);
                    break;
                }
            }
        }
    }

    public List<Message> process() throws InterruptedException {
        List<Message> deliverable = new ArrayList<Message>();
        lock.lock(); // Lock access to the buffer
        try {
            while(recordQueue.isEmpty() && buffer.isEmpty())
                notEmpty.await(1, TimeUnit.MILLISECONDS);

            queueToBuffer();
            // The buffer can be empty if a placeholder is rejected
            if (buffer.isEmpty())
                return deliverable;

            MessageRecord record = buffer.getFirst();
            long expectedSequence = getSequence(record.id.getOriginator());
            if (record.id.getSequence() > expectedSequence) {
                if(!record.id.getOriginator().equals(hitab.localAddress)) {
                    System.out.println("SEND SEQUENCE REQUEST - " + record.id.getOriginator() + " Expected Sequence := " + expectedSequence + " | Actual Sequence := " + record.id);
                    hitab.sendSequenceRequest(record.id.getOriginator(), expectedSequence);
                    isBlocked = true;
                    sequenceBlock.set(expectedSequence);
                    originBlock = record.id.getOriginator();
                }
                notEmpty.await(hitab.getRequestTimeout(), TimeUnit.MILLISECONDS);
                return deliverable;
            }

            // If blocked return nothing.  We should not be able to deliver any messages to the application until
            // the request sequence has been retrieved
            if (isBlocked)
                return deliverable;

            if (record.placeholder) {
                if(!record.id.getOriginator().equals(hitab.localAddress)) {
                    System.out.println("SEND PLACEHOLDER REQUEST | " + record.id + " | " + System.nanoTime());
                    hitab.sendPlaceholderRequest(record.id, record.ackInformer);
                }
                notEmpty.await(hitab.getRequestTimeout(), TimeUnit.MILLISECONDS);
                return deliverable;
            }

            Iterator<MessageRecord> i = buffer.iterator();
            while (i.hasNext()) {
                record = i.next();
                if(!record.placeholder && hitab.getCurrentTime() >= record.deliveryTime) {
                    expectedSequence = getSequence(record.id.getOriginator());
                    if (record.id.getSequence() <= expectedSequence) {
                        if (record.id.getSequence() == expectedSequence)
                            updateSequence(record);
                        deliverable.add(record.message);
                        i.remove();
                        lastDeliveredMessage = record;
                    } else {
                        System.out.println("Unexpected sequence number | Actual := " + record.id.getSequence() + " | Expected := " + expectedSequence);
                        break;
                    }
                } else {
                    notEmpty.await(1, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        }
        finally {
            lock.unlock();
        }
        return deliverable;
    }


    private void processBlockedMessage(MessageRecord record) {
        if (originBlock != null && record.id.getOriginator().equals(originBlock) && record.id.getSequence() == sequenceBlock.longValue()) {
            System.out.println("Blocking message received! | " + record.id);
            isBlocked = false;
            sequenceBlock.set(-1);
            originBlock = null;
        }
    }

    private void queueToBuffer() {
        MessageRecord newRecord;
        while ((newRecord = recordQueue.poll()) != null) {
            processBlockedMessage(newRecord);

            if (newRecord.placeholder)
                addPlaceholderToBuffer(newRecord);
            else
                addMessageToBuffer(newRecord);
        }
    }

    public long getSequence(Address origin) {
        return sequenceRecord.get(origin).longValue();
    }

    private void updateSequence(MessageRecord record) {
        sequenceRecord.get(record.id.getOriginator()).incrementAndGet();
    }

    private void updateSequences(View view) {
        for (Address address : view.getMembers()) {
            synchronized (sequenceRecord) {
                if (!sequenceRecord.containsKey(address))
                    sequenceRecord.put(address, new AtomicLong());
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
        return id.getSequence() < getSequence(id.getOriginator());
    }

    private boolean oldSequence(MessageRecord record) {
        return oldSequence(record.id);
    }

    final class MessageRecord {
        final MessageId id;
        final Message message;
        final Address ackInformer;
        final boolean placeholder;
        volatile long deliveryTime;

        MessageRecord(MessageId id, Message message, Address ackInformer, boolean placeholder, long deliveryTime) {
            this.id = id;
            this.message = message;
            this.ackInformer = ackInformer;
            this.placeholder = placeholder;
            this.deliveryTime = deliveryTime;
        }

        MessageRecord(Message message) {
            HiTabHeader header = (HiTabHeader) message.getHeader(ClassConfigurator.getProtocolId(HiTab.class));
            this.id = header.getId();
            this.message = message;
            this.ackInformer = header.getAckInformer();
            this.placeholder = false;
            this.deliveryTime = -1;
        }

        // Placeholder constructor
        MessageRecord(Address ackInformer, MessageId id) {
            this.id = id;
            this.message = null;
            this.ackInformer = ackInformer;
            this.placeholder = true;
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
                    ", deliveryTime=" + deliveryTime +
                    "} " + super.toString();
        }
    }
}