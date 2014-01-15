package org.jgroups.protocols.RMSysIntegratedNMC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SenderManager {
    private final PCSynch clock;
    private final int numberOfAcks;
    private final VectorClock vectorClock = new VectorClock();
    private final AtomicInteger sequence = new AtomicInteger();
    // Stores ids of messages that need to be acked
    private final Queue<MessageId> ackQueue = new ConcurrentLinkedQueue<MessageId>();

    public SenderManager(PCSynch clock, int numberOfAcks) {
        this.clock = clock;
        this.numberOfAcks = numberOfAcks;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public int nextSequence() {
        return sequence.getAndIncrement();
    }

    public VectorClock newBroadcast(MessageId id) {
        VectorClock oldClock = VectorClock.copyVectorClock(vectorClock);
        vectorClock.setLastBroadcast(id);
        return oldClock;
    }

    public void addMessageToAck(MessageId id) {
        ackQueue.add(id);
    }

    public int numberOfAcksWaiting() {
        return ackQueue.size();
    }

    public Collection<MessageId> getIdsToAck() {
        MessageId id;
        int count = 0;
        Boolean flag = null;
        Collection<MessageId> acks = new ArrayList<MessageId>();
        while ((id = ackQueue.peek()) != null && count < numberOfAcks) {
            // If this nodes clock time is < the timestamp of the sent message then we can't ack this message yet
            // Occurs because of the error rate encountered between distributed clocks
            if (clock.getTime() < id.getTimestamp()) {
                System.out.println("clock.getTime() < id.getTimestamp()");
                break;
            }
            id = ackQueue.poll();
            flag = acks.add(id);
            sendAck(id);
            count++;
        }
        return acks;
    }

    private void sendAck(MessageId id) {
        vectorClock.getAckRecord().put(id.getOriginator(), id);
    }
}