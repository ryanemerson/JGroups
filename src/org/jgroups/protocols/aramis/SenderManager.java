package org.jgroups.protocols.aramis;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class stores records associated with each atomic broadcast message sent using Aramis and Base.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class SenderManager {
    private final PCSynch clock;
    private final int numberOfAcks;
    private final VectorClock vectorClock = new VectorClock();
    private final AtomicInteger sequence = new AtomicInteger();
    // Stores ids of messages that need to be acked
    private final Set<MessageId> receivedMessages = Collections.synchronizedSortedSet(new TreeSet<MessageId>());
    private final Set<MessageId> ackSet = Collections.synchronizedSortedSet(new TreeSet<MessageId>());

    public SenderManager(PCSynch clock, int numberOfAcks) {
        this.clock = clock;
        this.numberOfAcks = numberOfAcks;
    }

    // Just for tests
    public VectorClock getLastVectorClock() {
        return VectorClock.copyVectorClock(vectorClock);
    }

    public RMCastHeader newEmptyBroadcast(Address localAddress, Collection<Address> destinations) {
        try {
            MessageId firstId = (MessageId) ((SortedSet) ackSet).first();
            long delay = clock.getTime() - firstId.getTimestamp();
            if (delay < 0)
                Util.sleep((int) Math.ceil(-delay / 1000000.0)); // Convert delay to positive
        } catch (NoSuchElementException e) {
            // If there are no messages to be acked return null
            return null;
        }

        synchronized (receivedMessages) {
            MessageId messageId = new MessageId(clock.getTime(), localAddress, -1);
            return RMCastHeader.createEmptyAckHeader(messageId, destinations,
                    VectorClock.copyVectorClock(vectorClock), getIdsToAck());
        }
    }

    public RMCastHeader newMessageBroadcast(Message message, short headerId, Address localAddress, NMCData data, Collection<Address> destinations, Queue<Message> localMsgQueue) {
        synchronized (receivedMessages) {
            MessageId messageId = new MessageId(clock.getTime(), localAddress, sequence.getAndIncrement());
            RMCastHeader header =  RMCastHeader.createBroadcastHeader(messageId, localAddress, 0, data, destinations,
                    getLatestVectorClock(messageId, destinations), getIdsToAck());
            message.putHeader(headerId, header);
            message.src(localAddress);
            localMsgQueue.add(message);
            return header;
        }
    }

    public boolean acksRequired() {
        return !ackSet.isEmpty();
    }

    public void addMessageToAck(MessageId id) {
        synchronized (receivedMessages) {
            receivedMessages.add(id);
            ackSet.add(id);
        }
    }

    private VectorClock getLatestVectorClock(MessageId id, Collection<Address> destinations) {
        Collection<MessageId> oldIds = new HashSet<MessageId>();
        for (Address destination : destinations) {
            // Continue to next destination if it is equal to this node
            if (destination.equals(id.getOriginator()))
                continue;

            for (MessageId receivedId : receivedMessages) {
                MessageId oldId = vectorClock.getMessagesReceived().get(destination);
                boolean oldTime = receivedId.getTimestamp() <= id.getTimestamp();


                // Only compare received messages from the current destination
                // Vector clock is meant to show the latest message received from each remote destination
                // As well as the previous message to be broadcast by this node.
                if (receivedId.getOriginator().equals(destination)) {
                    if (oldTime && (oldId == null || receivedId.getTimestamp() > oldId.getTimestamp()))
                        vectorClock.getMessagesReceived().put(destination, receivedId);
                    else if (oldTime)
                        oldIds.add(receivedId); // Not the most recent id before this ids timestamp so remove
                    else
                        break; // Time of message is after this id.timestamp so exit the loop
                }
            }
        }
        // Remove all of the ids from the set that are older then the current messages timestamp
        receivedMessages.removeAll(oldIds);

        VectorClock oldClock = VectorClock.copyVectorClock(vectorClock);
        vectorClock.setLastBroadcast(id);
        return oldClock;
    }

    private Collection<MessageId> getIdsToAck() {
        int count = 0;
        Collection<MessageId> acks = new ArrayList<MessageId>();
        for (MessageId id : ackSet) {
            if (clock.getTime() < id.getTimestamp() || count >= numberOfAcks)
                break;

            acks.add(id);
            count++;
        }
        ackSet.removeAll(acks);
        return acks;
    }
}