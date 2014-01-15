package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.*;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class responsible for holding messages until they are ready to deliver
 *
 * @author ryan
 * @since 4.0
 */
public class DeliveryManager {
    // Stores message records before they are processed
    private final Map<MessageId, MessageRecord> messageRecords = new HashMap<MessageId, MessageRecord>();
    private final SortedSet<MessageRecord> deliverySet = new TreeSet<MessageRecord>(new MessageRecordComparator());
    private final Map<Address, MessageId> deliveredMsgRecord = new HashMap<Address, MessageId>();
    private final Log log = LogFactory.getLog(RMSys.class);
    private final RMSys rmSys;
    private final Profiler profiler;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition messageReceived = lock.newCondition();

    private volatile MessageId lastDelivered;

    public DeliveryManager(RMSys rmSys, Profiler profiler) {
        this.rmSys = rmSys;
        this.profiler = profiler;
    }

    public void addMessage(Message message) {
        MessageRecord record = new MessageRecord(message);
        calculateDeliveryTime(record);

        long start = System.nanoTime();
        lock.lock();
        try {
            long fin = System.nanoTime();
            long calc = fin - start;
            if (calc > 3000000)
                log.debug("Lock Aquire time := " + calc);

            if (log.isDebugEnabled())
                log.debug("Message added | " + record);

            // Process vc & acks at the start, so that placeholders are always created before a message is added to the deliverySet
            processVectorClock(record);
            processAcks(record);

            MessageRecord existingRecord = messageRecords.get(record.id);
            if (existingRecord == null) {
                log.debug("New MessageRecord created | " + record);
                existingRecord = record;
            } else {
                log.debug("Message added to existing record | " + existingRecord);
                existingRecord.addMessage(record);
                deliverySet.remove(existingRecord);
            }
            deliverySet.add(existingRecord);
            messageRecords.put(existingRecord.id, existingRecord);

            rmSys.handleAcks(existingRecord.getHeader());
            if (deliverySet.first().isDeliverable())
                messageReceived.signal();
        } finally {
            lock.unlock();
        }
    }

    public List<Message> getDeliverableMessages() throws InterruptedException {
        final List<Message> deliverable = new ArrayList<Message>();
        lock.lock();
        try {
            if (deliverySet.isEmpty())
                messageReceived.await();

            MessageRecord record;
            while (!deliverySet.isEmpty() && (record = deliverySet.first()).isDeliverable()) {
                if (!deliverySet.remove(record))
                    log.debug("Record not removed | " + record + " | \n\n\t\t\t\t\t" + toString());
                messageRecords.remove(record.id);
                deliverable.add(record.message);
                deliveredMsgRecord.put(record.id.getOriginator(), record.id);
                lastDelivered = record.id;

                int delay = (int) Math.ceil(rmSys.getClock().getTime() - record.id.getTimestamp() / 1000000.0);
                // Ensure that the stored delay is not negative (occurs due to clock skew)
                profiler.addDeliveryLatency(delay < 0 ? 0 : delay); // Store delivery latency in milliseconds

                log.debug("Deliver Msg := " + record.id + " | ackRecord := " + record.ackRecord + " | Deliverable := " + record.isDeliverable());
            }
        } finally {
            lock.unlock();
        }
        return deliverable;
    }

    public void processEmptyAckMessage(RMCastHeader header) {
        lock.lock();
        try {
            if (log.isDebugEnabled())
                log.debug("Empty Ack Message received | Acks := " + header.getAcks());

            processAcks(header.getId().getOriginator(), header.getAcks());

            if (deliverySet.first().isDeliverable())
                messageReceived.signal();
        } finally {
            lock.unlock();
        }
    }

    private void processVectorClock(MessageRecord record) {
        VectorClock vc = record.getHeader().getVectorClock();
        if (vc.getLastBroadcast() != null)
            createPlaceholder(vc.getLastBroadcast());

        for (MessageId id : vc.getAckRecord().values())
            createPlaceholder(id);
    }

    private void processAcks(MessageRecord record) {
        if (record.id.getOriginator().equals(rmSys.getLocalAddress()))
            return;

        processAcks(record.id.getOriginator(), record.getHeader().getAcks());
    }

    private void processAcks(Address source, Collection<MessageId> acks) {
        for (MessageId ackId : acks) {
            MessageRecord existingRecord = messageRecords.get(ackId);
            if (existingRecord == null)
                createPlaceholder(source, ackId, true);
            else
                existingRecord.addAck(source);
        }
    }

    private void createPlaceholder(MessageId id) {
        if (!messageRecords.containsKey(id))
            createPlaceholder(null, id, false);
    }

    private void createPlaceholder(Address ackSource, MessageId id, boolean ack) {
        // If id is older than the last delivered message, from the originator node, than do not create a placeholder
        if (id.compareTo(lastDelivered) <= 0)
            return;

        MessageRecord placeholderRecord = new MessageRecord(id, ackSource);
        deliverySet.add(placeholderRecord);
        messageRecords.put(placeholderRecord.id, placeholderRecord);

        if (ack)
            placeholderRecord.addAck(ackSource);
    }

    private void deliveryExpired(MessageRecord record) {
        record.timedOut = true;
        // TODO implement this
        if (log.isDebugEnabled())
            log.debug("Delivery expired := " + record);
    }

    // TODO Calculate the delivery time at the sending node - Allows for less data to be sent in the header
    private void calculateDeliveryTime(MessageRecord record) {
        RMCastHeader header = record.getHeader();
        NMCData data = header.getNmcData();

        long startTime = header.getId().getTimestamp();
        long delay = TimeUnit.MILLISECONDS.toNanos(Math.max(data.getCapD(), data.getXMax() + data.getCapS()));
        delay = delay + rmSys.getClock().getMaximumError();
        record.deliveryTime = startTime + delay;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        List<MessageRecord> records = new ArrayList<MessageRecord>(deliverySet);
        int upperLimit = deliverySet.size() > 9 ? 10 : deliverySet.size();
        for (int i = 0; i < upperLimit; i++) {
            sb.append(records.get(i));
            sb.append("\n\n");
        }

        return "DeliveryManager{" +
                "deliverySet=" + sb +
                '}';
    }

    final class MessageRecord implements Delayed {
        final MessageId id;
        volatile Message message;
        volatile long deliveryTime;
        volatile boolean timedOut;
        volatile Map<Address, Boolean> ackRecord;

        // Create placeholder record
        MessageRecord(MessageId id, Address ackSource) {
            this.id = id;
            this.ackRecord = new HashMap<Address, Boolean>();

            // If ackSource == null, then this record has been created based upon a vector clock not an ack,
            // therefore we don't want to add a entry to the ackRecord
            if (ackSource != null)
                ackRecord.put(ackSource, true);
            log.debug("Placeholder created := " + this);
        }

        MessageRecord(Message message) {
            RMCastHeader header = (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(RMSys.class));
            this.id = header.getId();
            this.message = message;
            this.deliveryTime = -1;
            this.timedOut = false;
            this.ackRecord = new HashMap<Address, Boolean>();
            initialiseMap(header);
        }

        RMCastHeader getHeader() {
            return message == null ? null : (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(RMSys.class));
        }

        // Param id must be the id of the message that contained the ack
        void addAck(Address source) {
            ackRecord.put(source, true);
            if (log.isTraceEnabled())
                log.trace("Add ack from  := " + source + " | For message := " + id + " | Deliverable := " + isDeliverable() + " | ackRecord := " + ackRecord);

            if (log.isDebugEnabled())
                log.debug("Add ack from  := " + source + " | For message := " + id + " | Deliverable := " + isDeliverable() +
                        " | ackRecord := " + ackRecord + " |\n\t\t\t deliverSet first := " + deliverySet.first());
        }

        void addMessage(MessageRecord newRecord) {
            message = newRecord.message;
            deliveryTime = newRecord.deliveryTime;
            initialiseMap((RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(RMSys.class)));
        }

        boolean isPlaceholder() {
            return message == null;
        }

        boolean isDeliverable() {
            if (isPlaceholder())
                return false;

            for (Map.Entry<Address, Boolean> entry : ackRecord.entrySet())
                if (!entry.getValue())
                    return false;

            MessageId previous = deliveredMsgRecord.get(id.getOriginator());
            if (previous != null && id.getSequence() != previous.getSequence() + 1)
                return false;

//            return vectorClockMessagesReceived();
            return true;
        }

        void initialiseMap(RMCastHeader header) {
            // Don't add the localaddress or source address as we should never receive an ack from these addresses
            // Also don't put to the map if there is already a mapping for an address, necessary in scenarios where
            // an ack(s) has been received before the actual message
            for (Address address : header.getDestinations())
                if (!address.equals(rmSys.getLocalAddress()) && !address.equals(id.getOriginator()) && !ackRecord.containsKey(address)) {
                    ackRecord.put(address, false);
                    log.debug("Add to ackRecord := " + address);
                }
        }

        boolean vectorClockMessagesReceived() {
            VectorClock vc = getHeader().getVectorClock();

//                if (vc.getLastBroadcast() != null && vc.getLastBroadcast().compareTo(delivered) > 0)
//
//
//                if (vc.getLastBroadcast() != null && delivered.compareTo(vc.getLastBroadcast()) > 0) {
////                    log.debug("VCMR() first if | delivered := " + delivered + " | vc.getLastBroadcast := " + vc.getLastBroadcast());
//                    return false;
//                }
//
//                for (MessageId vcAck : vc.getAckRecord().values())
//                    if (delivered.compareTo(vcAck) > 0)
//                        return false;
            return true;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deliveryTime - rmSys.getNMC().getClockTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Long.valueOf(getDelay(TimeUnit.NANOSECONDS)).compareTo(delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord record = (MessageRecord) o;

            if (id != null ? !id.equals(record.id) : record.id != null) return false;

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
                    ", deliveryTime=" + deliveryTime +
                    ", ackRecord=" + ackRecord +
                    ", isPlaceholder()=" + isPlaceholder() +
                    ", isDeliverable()=" + isDeliverable() +
                    ", getDelay()=" + getDelay(TimeUnit.MILLISECONDS) +
                    (getHeader() == null ? "" : ", vectorClock=" + getHeader().getVectorClock()) +
                    '}';
        }
    }

    final class MessageRecordComparator implements Comparator<MessageRecord> {
        @Override
        public int compare(MessageRecord record1, MessageRecord record2) {
            if (record1 == null)
                return record2 == null ? 0 : 1;
            else if (record2 == null)
                return -1;

            return record1.id.compareTo(record2.id);
        }
    }
}