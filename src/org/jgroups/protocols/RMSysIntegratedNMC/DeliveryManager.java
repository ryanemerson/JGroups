package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.*;
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
    private final Comparator<MessageRecord> COMPARATOR = new MessageRecordComparator();
    // Stores message records before they are processed
    private final Map<MessageId, MessageRecord> messageRecords = new HashMap<MessageId, MessageRecord>();
    private final SortedSet<MessageRecord> deliverySet = new TreeSet<MessageRecord>(COMPARATOR);
    // Synchronised because it is used for a single operation outside of a synchonised block (hasMessageExpired())
    private final Map<Address, MessageId> deliveredMsgRecord = Collections.synchronizedMap(new HashMap<Address, MessageId>());
    // Sequences that have been received but not yet delivered
    private final Map<Address, Set<Long>> receivedSeqRecord = new HashMap<Address, Set<Long>>();
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

    public RMCastHeader addLocalMessage(SenderManager senderManager, Message message, Address localAddress, NMCData data,
                                        short rmsysId, Collection<Address> destinations) {
        lock.lock();
        try {
            RMCastHeader header = senderManager.newMessageBroadcast(localAddress, data, destinations);
            message.putHeader(rmsysId, header);

            // Copy necessary to prevent message.setDest in deliver() from affecting broadcasts of copies > 0
            // Without this the RMSys.broadcastMessage() will throw an exception if a message has been delivered
            // before all of it's copies have been broadcast, this is because the msg destination is set to a single destination in deliver()
            message = message.copy();
            MessageRecord record = new MessageRecord(message);
            calculateDeliveryTime(record);
            addRecordToDeliverySet(record);

            if (deliverySet.first().isDeliverable())
                messageReceived.signal();

            if (log.isDebugEnabled())
                log.debug("Local Message added | " + record + (deliverySet.isEmpty() ? "" : " | deliverySet 1st := " + deliverySet.first()));

            return header;
        } finally {
            lock.unlock();
        }
    }

    public void addMessage(Message message) {
        message = message.copy();
        MessageRecord record = new MessageRecord(message);
        calculateDeliveryTime(record);

        lock.lock();
        try {
            if (hasMessageExpired(record.getHeader())) {
                if (log.isInfoEnabled())
                    log.info("An old message has been sent to the DeliveryManager | Message and its records discarded");
                rmSys.collectGarbage(record.id); // Remove records created in RMSys
                return;
            }

            if (log.isDebugEnabled())
                log.debug("Message added | " + record + (deliverySet.isEmpty() ? "" : " | deliverySet 1st := " + deliverySet.first()));

            // Process vc & acks at the start, so that placeholders are always created before a message is added to the deliverySet
            processAcks(record);
            processVectorClock(record);
            handleNewMessageRecord(record);
            rmSys.handleAcks(record.getHeader());

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
                MessageId id = record.id;
                if (!deliverySet.remove(record))
                    log.error("Record not removed from the delivery Set := " + record.id);
                messageRecords.remove(id);
                deliverable.add(record.message);
                deliveredMsgRecord.put(id.getOriginator(), id);
                lastDelivered = id;
                removeReceivedSeq(id);

                int delay = (int) Math.ceil((rmSys.getClock().getTime() - record.id.getTimestamp()) / 1000000.0);
                // Ensure that the stored delay is not negative (occurs due to clock skew) SHOULD be irrelevant
                profiler.addDeliveryLatency(delay); // Store delivery latency in milliseconds
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
            processVectorClock(header.getVectorClock());

            if (deliverySet.first().isDeliverable())
                messageReceived.signal();
        } finally {
            lock.unlock();
        }
    }

    public boolean hasMessageExpired(RMCastHeader header) {
        MessageId messageId = header.getId();
        MessageId lastDeliveredId = deliveredMsgRecord.get(messageId.getOriginator());

        boolean result = lastDeliveredId != null && lastDeliveredId.getSequence() >= messageId.getSequence();
        if (result && log.isErrorEnabled())
            log.debug("MSG received that has a seq < the last message delivered from " + lastDeliveredId.getOriginator() +
                    " therefore this message is ignored");

        return result;
    }

    private void handleNewMessageRecord(MessageRecord record) {
        MessageRecord existingRecord = messageRecords.get(record.id);
        if (existingRecord == null) {
            log.debug("New MessageRecord created | " + record);
            MessageId placeholderId = new MessageId(-1, record.id.getOriginator(), record.id.getSequence());
            MessageRecord placeholderRecord = messageRecords.get(placeholderId);
            if (placeholderRecord != null) {
                messageRecords.remove(placeholderId);
                deliverySet.remove(placeholderRecord);
            }
            existingRecord = record;
            addRecordToDeliverySet(existingRecord);
        } else {
            log.debug("Message added to existing record | " + existingRecord);
            existingRecord.addMessage(record);
        }
    }

    private void addRecordToDeliverySet(MessageRecord record) {
        deliverySet.add(record);
        messageRecords.put(record.id, record);
        getReceivedSeqRecord(record.id.getOriginator()).add(record.id.getSequence());
    }

    private void processVectorClock(MessageRecord record) {
        processVectorClock(record.getHeader().getVectorClock());
    }

    private void processVectorClock(VectorClock vc) {
        if (vc.getLastBroadcast() != null)
            createPlaceholder(vc.getLastBroadcast());

        for (MessageId id : vc.getMessagesReceived().values()) {
            createPlaceholder(id);
            checkForMissingSequences(id, vc.getLastBroadcast());
        }
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
        if (id.getOriginator().equals(rmSys.getLocalAddress()))
            return;

        if (!messageRecords.containsKey(id))
            createPlaceholder(null, id, false);
    }

    private void createPlaceholder(Address ackSource, MessageId id, boolean ack) {
        // If id is older than the last delivered message, from the originator node, than do not create a placeholder
        if (id.compareTo(lastDelivered) <= 0 && id.compareTo(deliveredMsgRecord.get(id.getOriginator())) <= 0)
            return;

        if (getReceivedSeqRecord(id.getOriginator()).contains(id.getSequence())) {
            MessageRecord record = new MessageRecord(id.getOriginator(), id.getSequence());
            messageReceived.signal();
            deliverySet.remove(record);
            messageRecords.remove(record.id);
        }

        MessageRecord placeholderRecord = new MessageRecord(id, ackSource);
        addRecordToDeliverySet(placeholderRecord);

        if (log.isDebugEnabled())
            log.debug("Normal Placeholder created := " + placeholderRecord);

        if (ack)
            placeholderRecord.addAck(ackSource);
    }

    private void checkForMissingSequences(MessageId id, MessageId lastBroadcast) {
        Address origin = id.getOriginator();
        long sequence = id.getSequence();
        long lastDeliveredSeq = -1;
        MessageId lastDelivered = deliveredMsgRecord.get(origin);
        if (lastDelivered != null)
            lastDeliveredSeq =  deliveredMsgRecord.get(origin).getSequence();

        if (sequence <= lastDeliveredSeq + 1)
            return;

        Set<Long> receivedSet = getReceivedSeqRecord(origin);
        for (long missingSeq = sequence; missingSeq > lastDeliveredSeq; missingSeq--) {
            if (!receivedSet.contains(missingSeq) && !(origin.equals(lastBroadcast.getOriginator()) && missingSeq == lastBroadcast.getSequence())) {
                MessageRecord seqPlaceholder = new MessageRecord(origin, missingSeq);
                addRecordToDeliverySet(seqPlaceholder);

                if (log.isInfoEnabled())
                    log.info("seqPlaceholder added to the delivery set | " + seqPlaceholder);
            }
        }
    }

    private Set<Long> getReceivedSeqRecord(Address origin) {
        Set<Long> receivedSet = receivedSeqRecord.get(origin);
        if (receivedSet == null) {
            receivedSet = new HashSet<Long>();
            receivedSeqRecord.put(origin, receivedSet);
        }
        return receivedSet;
    }

    private void removeReceivedSeq(MessageId id) {
        Set<Long> seqSet = receivedSeqRecord.get(id.getOriginator());
        if (seqSet != null)
            seqSet.remove(id.getSequence());
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

    final class MessageRecord {
        final MessageId id;
        volatile Message message;
        volatile long deliveryTime;
        volatile boolean timedOut;
        volatile Map<Address, Boolean> ackRecord;

        MessageRecord(Address origin, long sequence) {
            this.id = new MessageId(-1, origin, sequence);
        }

        // Create placeholder record
        MessageRecord(MessageId id, Address ackSource) {
            this.id = id;
            this.ackRecord = new HashMap<Address, Boolean>();

            // If ackSource == null, then this record has been created based upon a vector clock not an ack,
            // therefore we don't want to add a entry to the ackRecord
            if (ackSource != null)
                ackRecord.put(ackSource, true);
            if (log.isTraceEnabled())
                log.trace("Placeholder created := " + this);
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
            if (id.getTimestamp() > rmSys.getClock().getTime())
                return false;

            if (isPlaceholder())
                return false;

            for (Map.Entry<Address, Boolean> entry : ackRecord.entrySet())
                if (!entry.getValue())
                    return false;

            MessageId previous = deliveredMsgRecord.get(id.getOriginator());
            return previous == null ||  id.getSequence() == previous.getSequence() + 1;
        }

        void initialiseMap(RMCastHeader header) {
            // Don't add the localaddress or source address as we should never receive an ack from these addresses
            // Also don't put to the map if there is already a mapping for an address, necessary in scenarios where
            // an ack(s) has been received before the actual message
            for (Address address : header.getDestinations())
                if (!address.equals(rmSys.getLocalAddress()) && !address.equals(id.getOriginator()) && !ackRecord.containsKey(address))
                    ackRecord.put(address, false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord record = (MessageRecord) o;
            return COMPARATOR.compare(this, record) == 0;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", deliveryTime=" + deliveryTime +
                    ", ackRecord=" + ackRecord +
                    ", isPlaceholder()=" + isPlaceholder() +
                    ", isDeliverable()=" + isDeliverable() +
                    (deliveredMsgRecord.get(id.getOriginator()) == null ? "" : ", lastDeliveredThisNode=" + deliveredMsgRecord.get(id.getOriginator()).getSequence()) +
                    (getHeader() == null ? "" : ", vectorClock=" + getHeader().getVectorClock()) +
                    '}';
        }
    }

    private class MessageRecordComparator implements Comparator<MessageRecord> {
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