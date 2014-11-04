package org.jgroups.protocols.aramis;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.tests.ABService.CrashedNodeInfiniteClients;
import org.jgroups.tests.ABService.InfiniteClients;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for holding messages until they are ready to be delivered.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class DeliveryManager {
    private final Comparator<MessageRecord> COMPARATOR = new MessageRecordComparator();
    private final Map<MessageId, MessageRecord> messageRecords = new HashMap<MessageId, MessageRecord>();
    private final TreeSet<MessageRecord> deliverySet = new TreeSet<MessageRecord>(COMPARATOR);
    // Synchronised because it is used for a single operation outside of a synchonised block (hasMessageExpired())
    private final Map<Address, MessageId> deliveredMsgRecord = Collections.synchronizedMap(new HashMap<Address, MessageId>());
    // Sequences that have been received but not yet delivered
    private final Map<Address, Set<Long>> receivedSeqRecord = new HashMap<Address, Set<Long>>();
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<Message>();
    private final Queue<MessageId> localMsgQueue = new ConcurrentLinkedQueue<MessageId>();
    private final Log log = LogFactory.getLog(Aramis.class);
    private final Aramis aramis;
    private final Profiler profiler;

    private volatile MessageRecord lastDelivered;

    public DeliveryManager(Aramis aramis, Profiler profiler) {
        this.aramis = aramis;
        this.profiler = profiler;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (deliverySet) {
                    System.out.println("---------------------------------------------------------------------------------");
                    int i = 0;
                    Iterator<MessageRecord> it = deliverySet.iterator();
                    while (i < 20 && it.hasNext()) {
                        MessageRecord record = it.next();
                        System.out.println(record);
                        System.out.println(record.isDeliverablePrint());
                        MessageId temp = localMsgQueue.poll();
                        MessageRecord r = checkDeliverySet(temp);
                        System.out.println("LOCAL QUEUE | [" + i + "]" + temp +
                                " | In MessageRecords := " + messageRecords.containsKey(temp) +
                                " | index in DS := " + (r != null ? deliverySet.headSet(r).size() : -1) +
                                " In deliverySet := " + r);
                        i++;
                    }
                    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                }
            }
        }));
    }

    // TODO remove
    private MessageRecord checkDeliverySet(MessageId id) {
        if (id == null)
            return null;

        for (MessageRecord r : deliverySet)
            if (r.id.equals(id))
                return r;
        return null;
    }

    // Aramis
    public RMCastHeader addLocalMessage(SenderManager senderManager, Message message, Address localAddress, NMCData data,
                                                                     short rmsysId, Collection<Address> destinations) {
        RMCastHeader header = senderManager.newMessageBroadcast(localAddress, data, destinations, localMsgQueue);
        message.putHeader(rmsysId, header);
        message.src(localAddress);
        addMessageToQueue(message);
        return header;
    }

    // Aramis
    public void addMessage(Message message) {
        addMessageToQueue(message);
    }

    public void processEmptyAckMessage(Message message) {
        addMessageToQueue(message);
    }

    public boolean hasMessageExpired(RMCastHeader header) {
        MessageId messageId = header.getId();
        MessageId lastDeliveredId = deliveredMsgRecord.get(messageId.getOriginator());

        boolean result = lastDeliveredId != null && lastDeliveredId.getSequence() >= messageId.getSequence();
        if (result && log.isDebugEnabled())
            log.debug(header.getId() + " has a seq < the last delivered message " + lastDeliveredId +
                    " therefore this message is ignored | header copy := " + header.getCopy());

        return result;
    }

    private void addMessageToQueue(Message message) {
        // Copy necessary to prevent message.setDest in deliver() from affecting broadcasts of copies > 0
        // Without this the Aramis.broadcastMessage() will throw an exception if a message has been delivered
        // before all of it's copies have been broadcast, this is because the msg destination is set to a single destination in deliver()
//        message = message.copy();
        messageQueue.add(message);
    }

    private void addLocalMessage(Message message) {
        MessageRecord record = new MessageRecord(message);

        calculateDeliveryTime(record);
        addRecordToDeliverySet(record);

        if (log.isDebugEnabled())
            log.debug("Local Message added | " + record + (deliverySet.isEmpty() ? "" : " | deliverySet 1st := " + deliverySet.first()));
    }

    private void addRemoteMessage(Message message) {
        MessageRecord record = new MessageRecord(message);

        // If empty ack message don't do anything else
        RMCastHeader header = record.getHeader();
        if (header.getType() == RMCastHeader.EMPTY_ACK_MESSAGE) {
            processAcks(header.getId().getOriginator(), header.getAcks());
            processVectorClock(header.getVectorClock());
            // Do we need to store the vc as well?? The same as normal msgs?

            if (log.isDebugEnabled())
                log.debug("Empty Ack Message received | Acks := " + header.getAcks());
            return;
        }

        if (hasMessageExpired(record.getHeader())) {
            if (log.isInfoEnabled())
                log.info("An old message has been sent to the DeliveryManager | Message and its records discarded | id := " + record.id);
            aramis.collectGarbage(record.id); // Remove records created in Aramis
            return;
        }

        // Process vc & acks at the start, so that placeholders are always created before a message is added to the deliverySet
        processAcks(record);
        processVectorClock(record);

        calculateDeliveryTime(record);
        handleNewMessageRecord(record);

        if (log.isDebugEnabled())
            log.debug("Message added | " + record + (deliverySet.isEmpty() ? "" : " | deliverySet 1st := " + deliverySet.first()));
    }

    public List<Message> getDeliverableMessages() throws InterruptedException {
        final List<Message> deliverable = new ArrayList<Message>();
        synchronized (deliverySet) {
            // Prevents the thread from constantly running
            if (messageQueue.isEmpty() && deliverySet.isEmpty()) {
                Util.sleep(1);
                return deliverable;
            }

            Message message;
            while ((message = messageQueue.poll()) != null) {
                if (message.src().equals(aramis.getLocalAddress()))
                    addLocalMessage(message);
                else
                    addRemoteMessage(message);
            }

            processDeliverySet(deliverable); // Deliver normal and timedout messages
            return deliverable;
        }
    }

    private void processDeliverySet(List<Message> deliverable) {
        MessageRecord record;
        Iterator<MessageRecord> i = deliverySet.iterator();
        while (i.hasNext()) {
            record = i.next();
            if (record.isDeliverable()) {
                i.remove();
                processMessageRecord(record, deliverable);
                continue;
            }
            break;
        }
    }

    private void processMessageRecord(MessageRecord record, Collection<Message> deliverable) {
        MessageId id = record.id;
        messageRecords.remove(id);
        removeReceivedSeq(id);

        if (lastDelivered != null && id.getTimestamp() < lastDelivered.id.getTimestamp()) {
            if (log.isWarnEnabled())
                log.warn("Msg rejected as a msg with a newer timestamp has already been delivered | rejected msg := " +
                        id + " | lastDelivered := " + lastDelivered.id + " | record " + record);
//                        + " | nextInSet := " + getNextRecordFromOrigin(record) + "\n ****** " + vectorCheckOutput(record.getHeader()));
            aramis.collectGarbage(id);
            profiler.messageRejected();

            // Hacks to make each test know that a message has been rejected.  Necessary so that each experiment will
            // terminate when x number of messages, minus the number of rejections, has been received.
            CrashedNodeInfiniteClients.msgsReceived.incrementAndGet();
            InfiniteClients.msgsReceived.incrementAndGet();
        } else {
            deliverable.add(record.message);
        }

        if (record.isLocal())
            localMsgQueue.poll();

        deliveredMsgRecord.put(id.getOriginator(), id);
        lastDelivered = record;

        int delay = (int) Math.ceil((aramis.getClock().getTime() - record.id.getTimestamp()) / 1000000.0);
        // Ensure that the stored delay is not negative (occurs due to clock skew) SHOULD be irrelevant
        profiler.addDeliveryLatency(delay); // Store delivery latency in milliseconds
    }

    private void handleNewMessageRecord(MessageRecord record) {
        MessageRecord existingRecord = messageRecords.get(record.id);
        if (existingRecord == null) {
            if (log.isTraceEnabled())
                log.trace("New MessageRecord created | " + record);

            MessageId placeholderId = new MessageId(-1, record.id.getOriginator(), record.id.getSequence());
            MessageRecord placeholderRecord = messageRecords.get(placeholderId);
            if (placeholderRecord != null) {
                messageRecords.remove(placeholderId);
                deliverySet.remove(placeholderRecord);
            }
            addRecordToDeliverySet(record);
        } else {
            if (log.isTraceEnabled())
                log.trace("Message added to existing record | " + existingRecord);

            existingRecord.addMessage(record);
        }
    }

    private void addRecordToDeliverySet(MessageRecord record) {
        record.insertionTime = aramis.getClock().getTime();
        if (!deliverySet.add(record))
            log.fatal("Record := " + record.id + " | not added to delivery set | addRecordToDeliverySet");
        messageRecords.put(record.id, record);
        getReceivedSeqRecord(record.id.getOriginator()).add(record.id.getSequence());
    }

    private void processVectorClock(MessageRecord record) {
        processVectorClock(record.getHeader().getVectorClock());
    }

    private void processVectorClock(VectorClock vc) {
        if (vc.getLastBroadcast() != null) {
            createPlaceholder(vc.getLastBroadcast());
            // Necessary to check for missing sequences from the sending node.
            // Without this, seq placeholders wont be made in situations where there are only two sending nodes
            checkForMissingSequences(vc.getLastBroadcast());

            // Check for missing sequences using last received values in the vector clock
            for (MessageId lastReceived : vc.getMessagesReceived().values())
                if (!lastReceived.getOriginator().equals(aramis.getLocalAddress())) // Ignore local messages as they are handled by the local queue
                    checkForMissingSequences(lastReceived);
        }
    }

    private void processAcks(MessageRecord record) {
        if (record.id.getOriginator().equals(aramis.getLocalAddress()))
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
        if (id.getOriginator().equals(aramis.getLocalAddress()))
            return;

        if (!messageRecords.containsKey(id))
            createPlaceholder(null, id, false);
    }

    private void createPlaceholder(Address ackSource, MessageId id, boolean ack) {
        MessageId ackSourceLastDelivered = deliveredMsgRecord.get(id.getOriginator());
        if (lastDelivered != null && id.compareTo(lastDelivered.id) <= 0)
            return;

        if (ackSourceLastDelivered != null && (id.compareTo(ackSourceLastDelivered) <= 0) || id.compareLocalOrder(ackSourceLastDelivered) <= 0)
            return;

        if (getReceivedSeqRecord(id.getOriginator()).contains(id.getSequence())) {
            MessageRecord record = new MessageRecord(id.getOriginator(), id.getSequence());
            deliverySet.remove(record);
            messageRecords.remove(record.id);
        }

        MessageRecord placeholderRecord = new MessageRecord(id, ackSource);
        addRecordToDeliverySet(placeholderRecord);

        if (log.isTraceEnabled())
            log.trace("Normal Placeholder created := " + placeholderRecord);

        profiler.ackPlaceholderCreated();

        if (ack)
            placeholderRecord.addAck(ackSource);
    }

    private void checkForMissingSequences(MessageId knownMessage) {
        Address origin = knownMessage.getOriginator();
        long sequence = knownMessage.getSequence();
        MessageId lastDelivered = deliveredMsgRecord.get(origin);

        if (lastDelivered == null)
            return;

        long lastDeliveredSeq = lastDelivered.getSequence();
        if (sequence <= lastDeliveredSeq)
            return;

        Set<Long> receivedSet = getReceivedSeqRecord(origin);
        for (long missingSeq = sequence - 1; missingSeq > lastDeliveredSeq; missingSeq--) {

            if (!receivedSet.contains(missingSeq)) {
                MessageRecord seqPlaceholder = new MessageRecord(origin, missingSeq);
                addRecordToDeliverySet(seqPlaceholder);

                if (log.isTraceEnabled())
                    log.trace("seqPlaceholder added to the delivery set | " + seqPlaceholder);

                profiler.seqPlaceholderCreated();
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

        long delay = header.getDestinations().size() > 2 ? data.getCapS() + data.getCapD() : data.getCapD();
        // Takes into consideration the broadcast time of an ack and the max possible delay before an ack is piggybacked or explicitly broadcast
        long ackWait = (2 * data.getEta()) + data.getOmega(); // Must be the same as the ackWait used in Aramis.class
        delay = (2 * delay) + ackWait;

        if (record.id.getOriginator().equals(aramis.getLocalAddress()))
            profiler.addDeliveryDelay(delay);

        delay = TimeUnit.MILLISECONDS.toNanos(delay) + (2 * aramis.getClock().getMaximumError()); // Convert to Nanos and add epislon
        record.deliveryTime = header.getId().getTimestamp() + delay;
    }

    private long getCalculatedDeliveryDelay(MessageRecord record) {
        RMCastHeader header = record.getHeader();
        NMCData data = header.getNmcData();

        long delay = header.getDestinations().size() > 2 ? data.getCapS() + data.getCapD() : data.getCapD();
        // Takes into consideration the broadcast time of an ack and the max possible delay before an ack is piggybacked or explicitly broadcast
        long ackWait = (2 * data.getEta()) + data.getOmega(); // Must be the same as the ackWait used in Aramis.class
        delay = (2 * delay) + ackWait;

        return TimeUnit.MILLISECONDS.toNanos(delay) + (2 * aramis.getClock().getMaximumError()); // Convert to Nanos and add epislon
    }
  /*
    Debug methods
    private String vectorCheckOutput(RMCastHeader header) {
        VectorClock vc = header.getVectorClock();
        String s = "";
        for (MessageId id : vc.getMessagesReceived().values())
            s += id + " | --- inQueue:= " + doesQueueContainTimedOutRecord(id);
        return s;
    }

    private boolean doesQueueContainTimedOutRecord(MessageId id) {
        synchronized (messageQueue) {
            List<MessageId> ids = new ArrayList<MessageId>();
            for (Message m : messageQueue)
                ids.add(((RMCastHeader) m.getHeader(ClassConfigurator.getProtocolId(Aramis.class))).getId());

            return ids.contains(id);
        }
    }

    private MessageRecord getNextRecordFromOrigin(MessageRecord record) {
        Iterator<MessageRecord> i = deliverySet.tailSet(record, false).iterator();
        while (i.hasNext()) {
            MessageRecord r = i.next();
            if (r.id.getOriginator().equals(record.id.getOriginator()))
                return r;
        }
        return null;
    }
    */

    final class MessageRecord implements Delayed {
        final MessageId id;
        volatile Message message;
        volatile long deliveryTime;
        volatile boolean timedOut;
        volatile Map<Address, Boolean> ackRecord;

        volatile long insertionTime;

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
            RMCastHeader header = (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(Aramis.class));
            this.id = header.getId();
            this.message = message;
            this.deliveryTime = -1;
            this.timedOut = false;
            this.ackRecord = new HashMap<Address, Boolean>();
            initialiseMap(header);
        }

        RMCastHeader getHeader() {
            return message == null ? null : (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(Aramis.class));
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
            initialiseMap((RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(Aramis.class)));
        }

        boolean isPlaceholder() {
            return message == null;
        }

        boolean allAcksReceived() {
            for (Boolean ackReceived : ackRecord.values())
                if (!ackReceived)
                    return false;
            return true;
        }

        boolean isLocal() {
            return id.getOriginator().equals(aramis.getLocalAddress());
        }

        boolean isTimedOut() {
            if (getDelay(TimeUnit.NANOSECONDS) > 0)
                return false;

            if (!timedOut) {
                timedOut = true; // Ensures that the log output only occurs once!
                profiler.messageTimedOut();

                if (log.isInfoEnabled()) {
                    MessageRecord nextRecord = deliverySet.higher(this);
                    MessageId nextId = nextRecord == null ? null : nextRecord.id;
                    log.info("Msg timedOut, mark ready to deliver | record := " + this.toStringDeliverable() +
                            " | \n" + getHeader().getNmcData() + " | delay := " + DeliveryManager.this.getCalculatedDeliveryDelay(this));
//                            + " | next := " + nextId + " | ^^^^^^^^^^^^^ \n" + vectorCheckOutput(this.getHeader()));
                }
            }
            return true;
        }

        boolean isDeliverable() {
            // Necessary to ensure that no remote messages are missed when a local message timesout!
            // i.e. A remote message exists in the local message's vc but not in the deliverySet
            if (!messageQueue.isEmpty())
                return false;

            if (id.getTimestamp() > aramis.getClock().getTime())
                return false;

            if (isPlaceholder())
                return false;

            if (!isLocal()) {
                MessageId oldestLocalMsg = localMsgQueue.peek();
                if (oldestLocalMsg != null && id.getTimestamp() > oldestLocalMsg.getTimestamp())
                    return false;
            }

            MessageId previous = deliveredMsgRecord.get(id.getOriginator());
            if (previous != null && id.getSequence() != previous.getSequence() + 1)
                return false;

            // Deliverable if all acks have been received or if this record has timedout.
            return allAcksReceived() || isTimedOut();
        }

        boolean isDeliverablePrint() {
            if (!messageQueue.isEmpty()) {
                System.out.println("MessageQueue is not empty! Can't deliver message");
                return false;
            }

            if (id.getTimestamp() > aramis.getClock().getTime()) {
                long t = aramis.getClock().getTime();
                System.out.println("This timestamp is > clock.time() | " + t + " | diff := " + (t - id.getTimestamp()) + "ns");
                return false;
            }

            if (isPlaceholder()) {
                System.out.println("isPlaceholder := true");
                return false;
            }

            if (!isLocal()) {
                MessageId oldestLocalMsg = localMsgQueue.peek();
                if (oldestLocalMsg != null && id.getTimestamp() > oldestLocalMsg.getTimestamp()) {
                    System.out.println("!isLocal && latestLocal < id.getTimestamp() | localMsg := " + oldestLocalMsg);
                    return false;
                }
            }

            MessageId previous = deliveredMsgRecord.get(id.getOriginator());
            if (previous != null && id.getSequence() != previous.getSequence() + 1) {
                System.out.println("This message's prior message has not been delivered | previous := " + previous);
                return false;
            }

            System.out.println("We've reached the end | timedOut := " + timedOut + " | allAcksReceived := " + allAcksReceived() + " | delay <= 0 := " + (getDelay(TimeUnit.NANOSECONDS) <= 0));
            return allAcksReceived() || getDelay(TimeUnit.NANOSECONDS) <= 0;
        }

        void initialiseMap(RMCastHeader header) {
            // Don't add the localaddress or source address as we should never receive an ack from these addresses
            // Also don't put to the map if there is already a mapping for an address, necessary in scenarios where
            // an ack(s) has been received before the actual message
            for (Address address : header.getDestinations())
                if (!address.equals(aramis.getLocalAddress()) && !address.equals(id.getOriginator()) && !ackRecord.containsKey(address))
                    ackRecord.put(address, false);
        }

        long getActualDeliveryDelay(TimeUnit timeUnit) {
            return timeUnit.convert(getCalculatedDeliveryDelay(TimeUnit.NANOSECONDS) + (aramis.getClock().getTime() - deliveryTime), TimeUnit.NANOSECONDS);
        }

        long getCalculatedDeliveryDelay(TimeUnit timeUnit) {
            return timeUnit.convert(deliveryTime - id.getTimestamp(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            if (this == delayed)
                return 0;

            return Long.signum(getDelay(TimeUnit.NANOSECONDS) - delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(deliveryTime - aramis.getClock().getTime(), TimeUnit.NANOSECONDS);
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
            return "\nMessageRecord{" +
                    "id=" + id +
                    ", deliveryTime=" + deliveryTime +
                    ", ackRecord=" + ackRecord +
                    ", insertionTime=" + insertionTime +
                    ", isPlaceholder()=" + isPlaceholder() +
                    ", isDeliverable()=" + isDeliverable() +
                    ", delay()=" + getDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", actualDeliveryDelay()=" + getActualDeliveryDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", calculatedDeliveryDelay()=" + getCalculatedDeliveryDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", timedOut=" + timedOut +
                    (deliveredMsgRecord.get(id.getOriginator()) == null ? "" : ", lastDeliveredThisNode=" + deliveredMsgRecord.get(id.getOriginator()).getSequence()) +
                    (getHeader() == null ? "" : ", vectorClock=" + getHeader().getVectorClock()) + "}";
        }

        // Method for debugging!
        public String toStringDeliverable() {
            return "\nMessageRecord{" +
                    "id=" + id +
                    ", deliveryTime=" + deliveryTime +
                    ", ackRecord=" + ackRecord +
                    ", insertionTime=" + insertionTime +
                    ", isPlaceholder()=" + isPlaceholder() +
                    ", isDeliverable()=" + isDeliverablePrint() +
                    ", delay()=" + getDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", actualDeliveryDelay()=" + getActualDeliveryDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", calculatedDeliveryDelay()=" + getCalculatedDeliveryDelay(TimeUnit.MILLISECONDS) + "ms" +
                    ", timedOut=" + timedOut +
                    (deliveredMsgRecord.get(id.getOriginator()) == null ? "" : ", lastDeliveredThisNode=" + deliveredMsgRecord.get(id.getOriginator()).getSequence()) +
                    (getHeader() == null ? "" : ", vectorClock=" + getHeader().getVectorClock()) + "}";
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