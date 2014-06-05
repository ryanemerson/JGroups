package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class FlowControl {

    private final int BUCKET_SIZE = 1;
    private final double DELTA_REDUCTION = 0.01; // K variable, must be >= 1. A higher value increases the amount the cumulative delay is reduced // In seconds e.g. 0.01 = 10ms
    private final ReentrantLock lock = new ReentrantLock(false);
    private final AtomicInteger bucketId = new AtomicInteger();
    private final NMC nmc;
    private final RMSys rmSys;

    private BucketWrapper buckets = new BucketWrapper();
    private FCDataWrapper flowData = new FCDataWrapper();
    private NMCData nmcData = null; // The most recent nmc data accessed by this object

    public FlowControl(RMSys rmSys, NMC nmc) {
        this.rmSys = rmSys;
        this.nmc = nmc;
    }

    public void addMessage(Message message) {
        lock.lock();
        try {
            MessageBucket bucket = buckets.current; // Assign after the initial wait as the bucket will have changed
            boolean bucketIsFull = bucket.addMessage(message);
            if (bucketIsFull) {
                bucket.delay();
                bucket.send();
            }
        } finally {
            lock.unlock();
        }
    }

    private class MessageBucket {
        final int id;
        final Message[] messages;
        volatile int messageIndex = 0;
        volatile long broadcastTime = -1;
        volatile boolean sent = false;


        public MessageBucket() {
            id = bucketId.getAndIncrement();
            messages = new Message[BUCKET_SIZE];
        }

        boolean addMessage(Message message) {
            messages[messageIndex++] = message;

            if (isFull()) {
                calculateBroadcastRate();
                calculateBroadcastTime();

                buckets.cycle();
                return true;
            }
            return false;
        }

        boolean isFull() {
            return messageIndex == BUCKET_SIZE;
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
//            delay = delay < 0 ? 0 : delay;
            delay = delay < 0 ? 0 : (delay > 0.1 ? 0.1 : delay); // Limit the max delay
//            delay = delay < 0 ? (flowData.cumulativeDelay > 0 ? flowData.cumulativeDelay : 0) : delay;

            long delayInNanos = delay == 0 ? 0 : (long) Math.ceil(delay * 1e+9); // Convert to nanoseconds * 1e+9 so that the delay can be added to the currentTime
            broadcastTime = rmSys.getClock().getTime() + delayInNanos;

            // This broadcast time can't be less than the previous bucket, therefore increase this broadcast time accordingly
            if (buckets.previous != null && broadcastTime < buckets.previous.broadcastTime) {
                broadcastTime = buckets.previous.broadcastTime + 1;
            }

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

                        // Necessary for the first bucket, prevents delta == infinity
                        if (flowData.broadcastRate == 0)
                            flowData.delta = 0;
                        else
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

            sent = true;
        }

        public void delay() {
            long delay = getDelay();

            double delayInMilli = delay / 1e+6;
            long milli = (long) delayInMilli;
            int nano = (int) ((delayInMilli - milli) * 1e+6);

            if (delay > 0)
                Util.sleep(milli, nano);
        }

        public long getDelay() {
            long delay = broadcastTime - rmSys.getClock().getTime();
            return delay < 0 ? 0 : delay;
        }

        @Override
        public String toString() {
            return "MessageBucket{" +
                    "id=" + id +
                    ", messageIndex=" + messageIndex +
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

        @Override
        public String toString() {
            return "FCDataWrapper{" +
                    "cumulativeDelay=" + cumulativeDelay +
                    ", delta=" + delta +
                    ", broadcastRate=" + broadcastRate +
                    ", exponentialResult=" + exponentialResult +
                    ", bucketDelay=" + bucketDelay +
                    '}';
        }
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