package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Message;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class FlowControl {

    private final int BUCKET_SIZE = 20;
    private final double DELTA_REDUCTION = 10; // K variable, must be >= 1. A higher value increases the amount the cumulative delay is reduced
    private final AtomicInteger bucketId = new AtomicInteger();
    private final NMC nmc;
    private final RMSys rmSys;

    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<Message>(500); // TODO Make capacity configurable
    private final DelayQueue<MessageBucket> bucketDelayQueue = new DelayQueue<MessageBucket>();

    private BucketWrapper buckets = new BucketWrapper();
    private FCDataWrapper flowData = new FCDataWrapper();
    private NMCData nmcData = null; // The most recent nmc data accessed by this object

    public FlowControl(RMSys rmSys, NMC nmc) {
        this.rmSys = rmSys;
        this.nmc = nmc;

        Executors.newSingleThreadExecutor().execute(new MessageHandler()); // TODO make so this can be destroyed when the RMSys protocol is removed
    }
    public void addMessage(Message message){
        messageQueue.add(message);
    }

    private class MessageHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                Message message;
                while ((message = messageQueue.poll()) != null)
                    buckets.current.addMessage(message);

                MessageBucket bucket;
                while ((bucket = bucketDelayQueue.poll()) != null)
                    bucket.send();
            }
        }
    }

    private class MessageBucket implements Delayed {
        final int id;
        final Message[] messages;
        volatile int messageIndex = 0;
        volatile long fullBucketTime = -1;
        volatile long broadcastTime = -1;

        public MessageBucket() {
            id = bucketId.getAndIncrement();
            messages = new Message[BUCKET_SIZE];
        }

        void addMessage(Message message) {
            messages[messageIndex++] = message;

            if (messageIndex == BUCKET_SIZE) {
                fullBucketTime = rmSys.getClock().getTime();
                calculateBroadcastRate();
                calculateBroadcastTime();

                bucketDelayQueue.add(this);
                buckets.cycle();
            }
        }

        void calculateBroadcastRate() {
            if (buckets.oldest != null && buckets.previous != null)
                flowData.broadcastRate = 1e+9 / ((double) (buckets.previous.broadcastTime - buckets.oldest.broadcastTime) / BUCKET_SIZE); // Number of messages broadcast per second
            else
                flowData.broadcastRate = 0.0;
        }

        void calculateBroadcastTime() {
            boolean newDelta = calculateDelta();
            double bucketDelay = newDelta ? flowData.delta * BUCKET_SIZE : flowData.bucketDelay;
            double delay = flowData.cumulativeDelay + bucketDelay;
            delay = delay < 0 ? 0 : delay;

            long delayInNanos = (long) Math.ceil(delay * 1e+9); // Convert to nanoseconds * 1e+9 so that the delay can be added to the fullBucketTime
            broadcastTime = fullBucketTime + delayInNanos;

            // This broadcast time can't be less than the previous bucket, therefore increase this broadcast time accordingly
            if (buckets.previous != null && broadcastTime < buckets.previous.broadcastTime)
                broadcastTime = buckets.previous.broadcastTime + 1;

            flowData.cumulativeDelay = delay;
            flowData.bucketDelay = bucketDelay;
        }

        // returns true if a new delta value is calculated, false if the old value is still relevant
        boolean calculateDelta() {
            NMCData newNMCData = nmc.getData();
            if (newNMCData.equals(nmcData)) {
                try {
                    // If the exponential result is different to the previous then it means that the number of latencies
                    // that have exceeded xMax has increased (can't decrease because xMax would have changed)
                    double exponentialResult = getExponentialResult();
                    if (exponentialResult != flowData.exponentialResult) {
                        flowData.delta = (1 / flowData.broadcastRate) * ((1 - exponentialResult) / exponentialResult);
                        flowData.exponentialResult = exponentialResult;
                        return true;
                    }
                } catch (Exception e) {
                    // If an exception is thrown by getExponentialResult then it means no latencies have exceeded Xrc
                }
                return false; // The old delta value will be used
            } else {
                flowData.delta = -DELTA_REDUCTION / BUCKET_SIZE;
                nmcData = newNMCData;
            }
            return true;
        }

        double getExponentialResult() throws Exception {
            double r = nmc.calculateR();
            int c = 1; // TODO make configurable

            // return the new broadcast rate (omega2)
            return Math.pow(Math.E, ((1 - r) / c));
        }

        void send() {
            for (Message message : messages)
                rmSys.sendRMCast(message);
        }

        @Override
        public int compareTo(Delayed delayed) {
            if (this == delayed)
                return 0;

            return Long.signum(getDelay(TimeUnit.NANOSECONDS) - delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(broadcastTime - rmSys.getClock().getTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public String toString() {
            return "MessageBucket{" +
                    "id=" + id +
                    ", messageIndex=" + messageIndex +
                    ", fullBucketTime=" + fullBucketTime +
                    ", broadcastTime=" + broadcastTime +
                    '}';
        }
    }

    private class FCDataWrapper {
        double cumulativeDelay = 0; // w in pseudocode
        double delta = 0.0;
        double broadcastRate = 0.0;
        double exponentialResult = 0.0;
        double bucketDelay = 0.0;
    }

    private class BucketWrapper {
        MessageBucket current;
        MessageBucket previous;
        MessageBucket oldest;

        public BucketWrapper() {
            current = new MessageBucket();
        }

        void cycle() {
            oldest = previous;
            previous = current;
            current = new MessageBucket();
        }
    }
}