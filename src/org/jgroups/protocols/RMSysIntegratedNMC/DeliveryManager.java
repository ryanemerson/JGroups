package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
    final private Queue<MessageRecord> recordQueue = new ConcurrentLinkedQueue<MessageRecord>();
    final private List<MessageRecord> buffer = new ArrayList<MessageRecord>(500);
    final private Queue<MessageRecord> rejectedMessages = new LinkedBlockingQueue<MessageRecord>();
    final private ReentrantLock lock = new ReentrantLock(true);
    final private Condition notEmpty = lock.newCondition();
    final private Log log = LogFactory.getLog(RMSys.class);
    final private NMC nmc;

    private volatile MessageRecord lastDelivered;

    public DeliveryManager(NMC nmc) {
        this.nmc = nmc;
    }

    public void addMessage(Message message) {
        recordQueue.add(new MessageRecord(message));
    }

    public List<Message> getDeliverableMessages() throws InterruptedException {
        List<Message> deliverable = new ArrayList<Message>();
        lock.lock(); // Lock access to the buffer
        try {
            while (recordQueue.isEmpty() && buffer.isEmpty())
                notEmpty.await(1, TimeUnit.MILLISECONDS);

            queueToBuffer();
            // The buffer can be empty if a message is rejected
            if (buffer.isEmpty())
                return deliverable;

            Iterator<MessageRecord> i = buffer.iterator();
            MessageRecord record;
            while (i.hasNext()) {
                record = i.next();
                if (record.deliveryTime >= nmc.getClockTime()) {
                    deliverable.add(record.message);
                    i.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return deliverable;
    }

    private void calculateDeliveryTime(MessageRecord record) {
        RMCastHeader header = record.getHeader();
        NMCData data = header.getNmcData();

        long startTime = header.getId().getTimestamp();
        long delay = TimeUnit.MILLISECONDS.toNanos(Math.max(data.getCapD(), data.getXMax() + data.getCapS()));
        record.deliveryTime = startTime + delay + nmc.getMaxClockError();
    }

    private void queueToBuffer() {
        MessageRecord newRecord;
        while ((newRecord = recordQueue.poll()) != null) {
            long currentTime = nmc.getClockTime();
            calculateDeliveryTime(newRecord);
            if (validDeliveryTime(currentTime, newRecord))
                addMessageToBuffer(newRecord);
            else
                rejectMessage(newRecord);
        }
    }

    private boolean validDeliveryTime(long currentTime, MessageRecord record) {
        return lastDelivered == null ||
                (record.deliveryTime >= currentTime && record.id.getTimestamp() >= lastDelivered.id.getTimestamp());
    }

    private void rejectMessage(MessageRecord record) {
        log.error("Message rejected | " + record.getHeader() + " | Delivery Time := " + record.deliveryTime);
        rejectedMessages.add(record); // TODO implement a means for the application to be notified of a rejected message
    }

    private void addMessageToBuffer(MessageRecord record) {
        // Add the message to the message store
        if (buffer.isEmpty()) {
            buffer.add(record);
            return;
        }

        // If message was created before the oldest message in the buffer then add to the start of the buffer
        if (record.id.getTimestamp() < buffer.get(0).id.getTimestamp()) {
            for (MessageRecord newerRecord : buffer) {
                if (record.deliveryTime > newerRecord.deliveryTime) {
                    newerRecord.deliveryTime = record.deliveryTime;
                } else {
                    break;
                }
            }
            buffer.add(0, record);
        }
        // If this message was created before all other messages add to the end of the buffer
        else if (record.id.getTimestamp() > buffer.get(buffer.size() - 1).id.getTimestamp()) {
            long lastMsgDeliveryTime = buffer.get(buffer.size() - 1).deliveryTime;
            if (lastMsgDeliveryTime > record.deliveryTime) {
                record.deliveryTime = lastMsgDeliveryTime;
            }
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
                    if (record.id.getTimestamp() == next.id.getTimestamp())
                        if (record.id.getOriginator().compareTo(next.id.getOriginator()) > 0)
                            index++;

                    for (int j = i; j < buffer.size(); j++) {
                        MessageRecord futureMessage = buffer.get(j);
                        if (record.deliveryTime > futureMessage.deliveryTime) {
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

    final class MessageRecord {
        final MessageId id;
        final Message message;
        volatile long deliveryTime;

        MessageRecord(Message message) {
            RMCastHeader header = (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(RMSys.class));
            this.id = header.getId();
            this.message = message;
            this.deliveryTime = -1;
        }

        RMCastHeader getHeader() {
            return (RMCastHeader) message.getHeader(ClassConfigurator.getProtocolId(RMSys.class));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (deliveryTime != that.deliveryTime) return false;
            if (id != null ? !id.equals(that.id) : that.id != null) return false;
            if (message != null ? !message.equals(that.message) : that.message != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + (message != null ? message.hashCode() : 0);
            result = 31 * result + (int) (deliveryTime ^ (deliveryTime >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", message=" + message +
                    ", deliveryTime=" + deliveryTime +
                    '}';
        }
    }
}