package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Class responsible for holding messages until they are ready to deliver
 *
 * @author ryan
 * @since 4.0
 */
public class DeliveryManager {
    // Stores message records before they are processed
    private final BlockingQueue<MessageRecord> deliveryQueue = new DelayQueue<MessageRecord>();
    private final Log log = LogFactory.getLog(RMSys.class);
    private final NMC nmc;
    private final Profiler profiler;

    public DeliveryManager(NMC nmc, Profiler profiler) {
        this.nmc = nmc;
        this.profiler = profiler;
    }

    public void addMessage(Message message) {
        MessageRecord record = new MessageRecord(message);
        calculateDeliveryTime(record);
        synchronized (deliveryQueue) { // TODO investigate ... without this missorderings can occur when two DTs are very close (2 decimal places)
            long delay = record.getDelay(TimeUnit.NANOSECONDS);
            if (delay > 0) {
                deliveryQueue.add(record);
            } else
                rejectMessage(record, delay);
        }
    }

    public List<Message> getDeliverableMessages() throws InterruptedException {
        final List<Message> deliverable = new ArrayList<Message>();
        MessageRecord record;
        while ((record = deliveryQueue.poll()) != null) {
            deliverable.add(record.message);
        }
        return deliverable;
    }

    // TODO Calculate the delivery time at the sending node - Allows for less data to be sent in the header
    private void calculateDeliveryTime(MessageRecord record) {
        RMCastHeader header = record.getHeader();
        NMCData data = header.getNmcData();

        long startTime = header.getId().getTimestamp();
        long delay = TimeUnit.MILLISECONDS.toNanos(Math.max(data.getCapD(), data.getXMax() + data.getCapS()));
        delay = delay + nmc.getMaxClockError();
        record.deliveryTime = startTime + delay;
        profiler.addDeliveryLatency((int) Math.ceil(delay / 1000000.0)); // Store delivery latency in milliseconds
    }

    private void rejectMessage(MessageRecord record, long delay) {
        log.error("Message rejected | " + record + " | Calculated Delivery Delay := " + delay + "ns");
        profiler.messageRejected();
    }

    final class MessageRecord implements Delayed {
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
        public long getDelay(TimeUnit unit) {
            return unit.convert(deliveryTime - nmc.getClockTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Long.valueOf(getDelay(TimeUnit.NANOSECONDS)).compareTo(delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", deliveryTime=" + deliveryTime +
                    '}';
        }
    }
}