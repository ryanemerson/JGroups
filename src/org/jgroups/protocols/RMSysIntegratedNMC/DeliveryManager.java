package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for holding messages until they are ready to deliver
 *
 * @author ryan
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
    private final Map<Address, VectorClock> receivedVectorClocks = new HashMap<Address, VectorClock>();
    private final DelayQueue<MessageRecord> timedOutQueue = new DelayQueue<MessageRecord>();
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<Message>();
    private final Log log = LogFactory.getLog(RMSys.class);
    private final DMProfiler dmProfiler = new DMProfiler();
    private final RMSys rmSys;
    private final Profiler profiler;

    private enum DeliveryType {V1, V2}
    private DeliveryType deliveryType = DeliveryType.V2;
    private volatile MessageRecord lastDelivered;
    private volatile MessageRecord lastTimeout;

    public DeliveryManager(RMSys rmSys, Profiler profiler) {
        this.rmSys = rmSys;
        this.profiler = profiler;

        // TODO remove
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("IN RUNNABLE!!!!!!!!!!!!!!");
                synchronized (deliverySet) {
                    List<MessageRecord> records = new ArrayList<MessageRecord>();
                    int count = 0;
                    for (MessageRecord record : deliverySet) {
                        if (count++ == 20)
                            break;

                        records.add(record);
                    }
                    System.out.println("Processing ----------------");
                    System.out.println("Total Time taken := " + TimeUnit.NANOSECONDS.toSeconds(dmProfiler.timeTaken));
                    System.out.println("Average time := " + (dmProfiler.timeTaken / dmProfiler.accessCount));
                    System.out.println("Longest time := " + dmProfiler.longestTime);
                    System.out.println("Shortest time := " + dmProfiler.shortestTime);

                    System.out.println("Queue Polling -------------------");
                    System.out.println("Total Time taken := " + TimeUnit.NANOSECONDS.toSeconds(dmProfiler.queuePollTime));
                    System.out.println("Average time := " + (dmProfiler.queuePollTime / dmProfiler.pollCount));
                    System.out.println("Longest := " + dmProfiler.longestPoll);
                    System.out.println("Shortest := " + dmProfiler.shortestPoll);

                    System.out.println("DeliverySet := " + records);
                }
            }
        }));
    }

    // RMSys
    public RMCastHeader addLocalMessage(SenderManager senderManager, Message message, Address localAddress, NMCData data,
                                        short rmsysId, Collection<Address> destinations) {
        RMCastHeader header = senderManager.newMessageBroadcast(localAddress, data, destinations);
        message.putHeader(rmsysId, header);
        message.src(localAddress);
        addMessageToQueue(message);
        return header;
    }

    // RMSys
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
        // Without this the RMSys.broadcastMessage() will throw an exception if a message has been delivered
        // before all of it's copies have been broadcast, this is because the msg destination is set to a single destination in deliver()
        message = message.copy();
        messageQueue.add(message);
    }

    private void addLocalMessage(Message message) {
        MessageRecord record = new MessageRecord(message);

        calculateDeliveryTime(record);
        addRecordToDeliverySet(record);
        updateDeliveryTimes(record);
        storeVectorClock(record);

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
            rmSys.collectGarbage(record.id); // Remove records created in RMSys
            return;
        }

        // Process vc & acks at the start, so that placeholders are always created before a message is added to the deliverySet
        processAcks(record);
        processVectorClock(record);

        calculateDeliveryTime(record);
        handleNewMessageRecord(record);
        rmSys.handleAcks(record.getHeader());

        if (log.isDebugEnabled())
            log.debug("Message added | " + record + (deliverySet.isEmpty() ? "" : " | deliverySet 1st := " + deliverySet.first()));

        log.debug("Message added | " + record.id);
    }

    public List<Message> getDeliverableMessages() throws InterruptedException {
        final List<Message> deliverable = new ArrayList<Message>();
        synchronized (deliverySet) {
            // Prevents the thread from constantly running
            if (messageQueue.isEmpty() && (timedOutQueue.isEmpty() || timedOutQueue.peek().getDelay(TimeUnit.NANOSECONDS) > 0)) {
                Util.sleep(1);
                return deliverable;
            }

            long s = System.nanoTime();

            Message message;
            while ((message = messageQueue.poll()) != null) {
                if (message.src().equals(rmSys.getLocalAddress()))
                    addLocalMessage(message);
                else
                    addRemoteMessage(message);
            }
            long f = System.nanoTime() - s;

            dmProfiler.queuePollTime += f;
            dmProfiler.longestPoll = f > dmProfiler.longestPoll ? f : dmProfiler.longestPoll;
            dmProfiler.shortestPoll = f < dmProfiler.shortestPoll ? f : dmProfiler.shortestPoll;
            dmProfiler.pollCount++;

            long st = System.nanoTime();

            switch (deliveryType) {
                case V1:
                    // OLD APPROACH
                    processDeliverySet(deliverable); // Deliver messages that can be delivered under normal conditions
                    List<MessageRecord> validPlaceholders = processTimeoutQueue(); // Make messages with an expired timeout deliverable and remove any blocking placeholders
                    processDeliverySet(deliverable); // Deliver expired messages
                    deliverySet.addAll(validPlaceholders); // Re-add the 'valid' placeholders
                    break;
                case V2:
                    // NEW APPROACH
                    processTimeoutQueueV2(); // Get the newest timedout message
                    processDeliverySetV2(deliverable); // Deliver normal and timedout messages
                    break;
            }

            long latency = System.nanoTime() - st;
            dmProfiler.timeTaken += latency;
            dmProfiler.longestTime = latency > dmProfiler.longestTime ? latency : dmProfiler.longestTime;
            dmProfiler.shortestTime = latency < dmProfiler.shortestTime ? latency : dmProfiler.shortestTime;
            dmProfiler.accessCount++;
            return deliverable;
        }
    }

    // ------------------- Version 1 of delivery conditions ------------------------------- //
    private void processDeliverySet(List<Message> deliverable) {
        MessageRecord record;
        while (!deliverySet.isEmpty() && (record = deliverySet.first()).isDeliverable()) {
            MessageId id = record.id;
            deliverySet.remove(record);
            messageRecords.remove(id);
            deliveredMsgRecord.put(id.getOriginator(), id);
            removeReceivedSeq(id);
            timedOutQueue.remove(record);

            if (lastDelivered != null && id.getTimestamp() < lastDelivered.id.getTimestamp()) {
                if (log.isWarnEnabled()) {
                    log.warn("Msg rejected as a msg with a newer timestamp has already been delivered | rejected msg := " +
                            id + " | lastDelivered := " + lastDelivered.id);
                }
            } else {
                deliverable.add(record.message);
            }
            lastDelivered = record;

            int delay = (int) Math.ceil((rmSys.getClock().getTime() - record.id.getTimestamp()) / 1000000.0);
            // Ensure that the stored delay is not negative (occurs due to clock skew) SHOULD be irrelevant
            profiler.addDeliveryLatency(delay); // Store delivery latency in milliseconds
        }
    }

    private List<MessageRecord> processTimeoutQueue() {
        List<MessageRecord> validPlaceholders = new ArrayList<MessageRecord>();
        MessageRecord record;
        MessageRecord lastTimeout = null;
        while ((record = timedOutQueue.poll()) != null) {

            record.timedOut = true;
            lastTimeout = record;

            if (log.isWarnEnabled() && !record.allAcksReceived())
                log.warn("Msg timedOut, mark ready to deliver | record := " + record);
        }

        // Set all messages with an expired deliveryTime to be deliverable and remove any blocking placeholders
        if (lastTimeout != null)
            validPlaceholders =  updateOlderMessages(lastTimeout);

        return validPlaceholders;
    }

    private List<MessageRecord> updateOlderMessages(MessageRecord timedOutRecord) {
        List<MessageRecord> validPlaceholders = new ArrayList<MessageRecord>();
        Iterator<MessageRecord> i = deliverySet.headSet(timedOutRecord).iterator();
        while(i.hasNext()) {
            MessageRecord record = i.next();

            if (!nodeIsAlive(record, timedOutRecord)) {
                i.remove();
                messageRecords.remove(record.id);
                removeReceivedSeq(record.id);
                if (log.isDebugEnabled())
                    log.debug("Placeholder removed from the delivery set forever | " + record.id);
            }
            // If ph is > then the last received msg contained in this msg's vc then remove it and add it to valid phs
            // Provision for phs that are greater than timedOutRecord - Reduces blocking
            else if (record.isPlaceholder() && placeholderIsIgnorable(timedOutRecord, record)) {
                if (log.isDebugEnabled())
                    log.debug("Future placeholder ignored during delivery | " + record.id);
                i.remove();
                validPlaceholders.add(record);
            }
        }
        return validPlaceholders;
    }

    // *********************** VERSION 2 of delivery conditions ************************************** //
    private void processTimeoutQueueV2() {
        MessageRecord record;
        while ((record = timedOutQueue.poll()) != null) {

            record.timedOut = true;
            lastTimeout = record;

            if (log.isInfoEnabled() && !record.allAcksReceived())
                log.info("Msg timedOut, mark ready to deliver | record := " + record);
        }
    }

    private void processDeliverySetV2(List<Message> deliverable) {
        if (deliverySet.isEmpty() || (lastTimeout == null && !deliverySet.first().isDeliverable()))
            return;

        MessageRecord record;
        Iterator<MessageRecord> i = deliverySet.iterator();
        while (i.hasNext()) {
            record = i.next();

            if (record.isDeliverable()) {
                MessageId id = record.id;
                i.remove();
                messageRecords.remove(id);
                deliveredMsgRecord.put(id.getOriginator(), id);
                removeReceivedSeq(id);
                timedOutQueue.remove(record);

                if (lastDelivered != null && id.getTimestamp() < lastDelivered.id.getTimestamp()) {
                    if (log.isWarnEnabled()) {
                        log.warn("Msg rejected as a msg with a newer timestamp has already been delivered | rejected msg := " +
                                id + " | lastDelivered := " + lastDelivered.id);
                    }
                } else {
                    deliverable.add(record.message);
                }
                lastDelivered = record;

                if (lastTimeout != null && lastTimeout.equals(record))
                    lastTimeout = null;

                int delay = (int) Math.ceil((rmSys.getClock().getTime() - record.id.getTimestamp()) / 1000000.0);
                // Ensure that the stored delay is not negative (occurs due to clock skew) SHOULD be irrelevant
                profiler.addDeliveryLatency(delay); // Store delivery latency in milliseconds
                continue;
            }

            if (record.isPlaceholder() && lastTimeout != null) {
                if (!nodeIsAlive(record, lastTimeout)) {
                    i.remove(); // Remove from the deliverySet forever
                    messageRecords.remove(record.id);
                    removeReceivedSeq(record.id);

                    if (log.isDebugEnabled())
                        log.debug("Placeholder removed from the delivery set forever | " + record.id);
                    continue;
                }

                // If ph is > then the last received msg contained in this msg's vc then ignore it this time
                // Provision for handling phs that are greater than lastTimeout - Reduces blocking
                if (placeholderIsIgnorable(lastTimeout, record)) {
                    if (log.isDebugEnabled())
                        log.debug("Future placeholder ignored during delivery | " + record.id);
                    continue;
                }
            }
            break;
        }
    }

    // -------------- Start of common methods ------------------------ //

    private boolean placeholderIsIgnorable(MessageRecord timedOutRecord, MessageRecord placeholder) {
        MessageId id = timedOutRecord.id;
        VectorClock vc = timedOutRecord.getHeader().getVectorClock();
        Address phOrigin = placeholder.id.getOriginator();

        try {
            if (id.compareLocalOrder(placeholder.id) < 0)
                return true; // return true if id seq < ph
        } catch (IllegalArgumentException e) {
        }

        MessageId vcMsg = null;
        try {
            vcMsg = vc.getMessagesReceived().get(phOrigin);
            if (vcMsg != null && vcMsg.compareLocalOrder(placeholder.id) <= 0)
                return true; // return true if vc msg < ph
        } catch (IllegalArgumentException e) {
        }

        // If timedout record created the placeholder, then it is safe to ignore the ph as the timedout record must come before the ph
        return timedOutRecord.getHeader().getAcks().contains(placeholder.id);
    }

    private boolean nodeIsAlive(MessageRecord currentRecord, MessageRecord timedOutRecord) {
        MessageId id = currentRecord.id;

        // If cr is <= to timedOutRecord, then we know the origin is still alive (or that the missing msgs have been broadcast and are waiting to arrive)
        if (id.getTimestamp() != -1 && id.compareTo(timedOutRecord.id) <= 0) // Handles normal ph and actual records
            return true;

        // If the currentRecord is a seq ph then we know that this msg is on it's way as a subsequent msg has been
        // received by this node or another node (we know of Seq from the vectorClock) therefore the msg will eventually arrive
        // as if the node has crashed during transmission, another node will complete the broadcast of at least one message copy
        if (currentRecord.isPlaceholder() && id.getTimestamp() == -1)
            return true;

        // Check if a newer message from the current records origin is known in the vector clocks
        for (Address address : receivedVectorClocks.keySet())
            if (newerMsgExists(currentRecord, address))
                return true;

        return false; // no evidence that the currentRecords origin is still alive
    }

    private boolean newerMsgExists(MessageRecord record, Address host) {
        Address origin = record.id.getOriginator();
        VectorClock vc = receivedVectorClocks.get(host);
        if (vc == null)
            return false; // Node might be alive however we have yet to receive any msgs so we have to say no

        // If an ack is missing from node x and the vc from x has not received a message > record.id
        // Then the node is classed as being crashed
        MessageId lastReceived = vc.getMessagesReceived().get(origin);
        return lastReceived != null && lastReceived.compareTo(record.id) > 0;
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
            updateDeliveryTimes(record);
        } else {
            if (log.isTraceEnabled())
                log.trace("Message added to existing record | " + existingRecord);

            existingRecord.addMessage(record);
            updateDeliveryTimes(existingRecord);
        }
    }

    private void addRecordToDeliverySet(MessageRecord record) {
        if (!deliverySet.add(record))
            log.fatal("Record := " + record.id + " | not added to delivery set | addRecordToDeliverySet");
        messageRecords.put(record.id, record);
        getReceivedSeqRecord(record.id.getOriginator()).add(record.id.getSequence());
    }

    private void updateDeliveryTimes(MessageRecord record) {
        timedOutQueue.add(record);
    }

    private void processVectorClock(MessageRecord record) {
        storeVectorClock(record);
        processVectorClock(record.getHeader().getVectorClock());
    }

    private void processVectorClock(VectorClock vc) {
        if (vc.getLastBroadcast() != null) {
            createPlaceholder(vc.getLastBroadcast());
            // Necessary to check for missing sequences from the sending node.
            // Without this, seq placeholders wont be made in situations where there are only two sending nodes
            checkForMissingSequences(vc.getLastBroadcast());
        }
    }

    private void storeVectorClock(MessageRecord record) {
        Address origin = record.id.getOriginator();
        VectorClock existingVC = receivedVectorClocks.get(origin);
        VectorClock vectorClock = record.getHeader().getVectorClock();

        if (existingVC == null || existingVC.getLastBroadcast() == null ||
                existingVC.getLastBroadcast().compareTo(vectorClock.getLastBroadcast()) < 0)
            receivedVectorClocks.put(origin, vectorClock);
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

        if (ack)
            placeholderRecord.addAck(ackSource);
    }

    private void checkForMissingSequences(MessageId lastBroadcast) {
        Address origin = lastBroadcast.getOriginator();
        long sequence = lastBroadcast.getSequence();
        long lastDeliveredSeq = -1;
        MessageId lastDelivered = deliveredMsgRecord.get(origin);
        if (lastDelivered != null)
            lastDeliveredSeq =  lastDelivered.getSequence();

        if (sequence <= lastDeliveredSeq)
            return;

        Set<Long> receivedSet = getReceivedSeqRecord(origin);
        for (long missingSeq = sequence; missingSeq > lastDeliveredSeq; missingSeq--) {
            // Stops the last broadcast from being added again (last broadcast is always processed first in vc)
            if (missingSeq == sequence)
                continue;

            if (!receivedSet.contains(missingSeq)) {
                MessageRecord seqPlaceholder = new MessageRecord(origin, missingSeq);
                addRecordToDeliverySet(seqPlaceholder);

                if (log.isTraceEnabled())
                    log.trace("seqPlaceholder added to the delivery set | " + seqPlaceholder);
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
        long ackWait = (2 * data.getEta()) + data.getOmega();
        long delay = Math.max(data.getCapD(), data.getXMax() + data.getCapS());
        // Takes into consideration the broadcast time of an ack and the max possible delay before an ack is piggybacked or explicitly broadcast
        delay = (2 * delay) + ackWait;
        delay = TimeUnit.MILLISECONDS.toNanos(delay) + rmSys.getClock().getMaximumError(); // Convert to Nanos and add epislon
        record.deliveryTime = startTime + delay;

        if (lastDelivered != null && record.deliveryTime < lastDelivered.deliveryTime)
            record.deliveryTime = lastDelivered.deliveryTime + 1;
    }

    final class MessageRecord implements Delayed {
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

        boolean allAcksReceived() {
            for (Boolean ackReceived : ackRecord.values())
                if (!ackReceived)
                    return false;
            return true;
        }

        boolean isDeliverable() {
            if (id.getTimestamp() > rmSys.getClock().getTime())
                return false;

            if (isPlaceholder())
                return false;

            MessageId previous = deliveredMsgRecord.get(id.getOriginator());
            if (previous != null && id.getSequence() != previous.getSequence() + 1)
                return false;

            return timedOut || allAcksReceived();
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
        public int compareTo(Delayed delayed) {
            if (this == delayed)
                return 0;

            return Long.signum(getDelay(TimeUnit.NANOSECONDS) - delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(deliveryTime - rmSys.getClock().getTime(), TimeUnit.NANOSECONDS);
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
                    ", isPlaceholder()=" + isPlaceholder() +
                    ", isDeliverable()=" + isDeliverable() +
                    ", delay()=" + getDelay(TimeUnit.MILLISECONDS) + "ms" +
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

    private class DMProfiler {
        volatile long timeTaken = 0;
        volatile long longestTime = 0;
        volatile long shortestTime = Long.MAX_VALUE;
        volatile long accessCount = 0;

        volatile long queuePollTime = 0;
        volatile long longestPoll = 0;
        volatile long shortestPoll = Long.MAX_VALUE;
        volatile long pollCount = 0;
    }
}