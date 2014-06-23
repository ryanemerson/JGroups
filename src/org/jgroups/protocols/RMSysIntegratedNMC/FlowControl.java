package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Message;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FlowControl {

    private final int BUCKET_SIZE = 1;
    private final double DELTA_UPPER_LIMIT = 0.01; // The max value of delta in seconds e.g. 0.01 = 10ms
    private final double DELTA_LOWER_LIMIT = 0.001; // The min value of delta in seconds
    private final ReentrantLock lock = new ReentrantLock(false);
    private final Condition signal = lock.newCondition();
    private final AtomicInteger bucketId = new AtomicInteger();
    private final NMC nmc;
    private final RMSys rmSys;

    private BucketWrapper buckets = new BucketWrapper();
    private FCDataWrapper flowData = new FCDataWrapper();
    private NMCData nmcData = null; // The most recent nmc data accessed by this object

    private final Profiler profiler = new Profiler(); // TODO remove
    private final boolean PROFILE_ENABLED = true;

    public FlowControl(RMSys rmSys, NMC nmc) {
        this.rmSys = rmSys;
        this.nmc = nmc;

        if (PROFILE_ENABLED) {
            // TODO remove
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Flow Control -------\n" + profiler);
                }
            }));
        }
    }

    public void addMessage(Message message) {
        lock.lock();
        try {
            MessageBucket bucket = buckets.current;
            boolean bucketIsFull = bucket.addMessage(message);
            if (bucketIsFull) {
                try {
                    bucket.delay();
                    bucket.send();
                } catch (InterruptedException e) {
                    System.out.println("Delay Exception: " + e);
                }
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
        volatile MessageBucket previous = null;


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

            if (bucketDelay > DELTA_UPPER_LIMIT) {
                if (PROFILE_ENABLED)
                    profiler.deltaLimitExceeded(bucketDelay);
                bucketDelay = DELTA_UPPER_LIMIT; // Reset the buckets delay to the upper limit
            }

            if (bucketDelay < DELTA_LOWER_LIMIT)
                bucketDelay = DELTA_LOWER_LIMIT;

            double delay = bucketDelay;
            long delayInNanos = delay == 0 ? 0 : (long) Math.ceil(delay * 1e+9); // Convert to nanoseconds * 1e+9 so that the delay can be added to the currentTime

            if (buckets.previous != null) {
                broadcastTime = buckets.previous.broadcastTime + delayInNanos;
                previous = buckets.previous;
            } else
                broadcastTime = rmSys.getClock().getTime() + delayInNanos;

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
                flowData.delta = DELTA_LOWER_LIMIT;
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
            signal.signalAll();
        }

        public void delay() throws InterruptedException {
            while ((id != 0 && previous != null && !previous.sent) || getDelay() > 0)
                signal.awaitNanos(getDelay());
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
        double delta = 0.0;
        double broadcastRate = 0.0;
        double exponentialResult = 0.0;
        double bucketDelay = 0.0;

        @Override
        public String toString() {
            return "FCDataWrapper{" +
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

    private class Profiler {
        int deltaExceeded = 0;
        double deltaExceededTotal = 0;
        double deltaHighest = Double.MIN_VALUE;
        double deltaLowest = Double.MAX_VALUE;
        int cumulativeLimit = 1; // 1 Second
        int cumulativeExceeded = 0;
        double cumulativeExceededTotal = 0;
        long delayTotal = 0;
        int msgCount = 0;

        public void deltaLimitExceeded(double bucketDelay) {
            deltaExceeded++;
            deltaHighest = Math.max(profiler.deltaHighest, bucketDelay);
            deltaLowest = Math.min(profiler.deltaLowest, bucketDelay);
            deltaExceededTotal += bucketDelay - DELTA_UPPER_LIMIT;
        }

        @Override
        public String toString() {
            return "Profiler{" +
                    "Delta Exceeded=" + deltaExceeded +
                    ", Delta Exceeded Average =" + (deltaExceeded > 0 ? (deltaExceededTotal / deltaExceeded) : 0) +
                    ", Delta Highest =" + (deltaHighest - DELTA_UPPER_LIMIT) +
                    ", Delta Lowest =" + (deltaLowest - DELTA_UPPER_LIMIT) +
                    ", w Limit=" + cumulativeLimit +
                    ", w Exceeded=" + cumulativeExceeded +
                    ", w Average=" + cumulativeExceededTotal +
                    ", Delay Average=" + TimeUnit.NANOSECONDS.toMillis(msgCount > 0 ? (delayTotal / msgCount) : 0) +
                    ", Msg Count=" + msgCount +
                    ", delayTotal =" + delayTotal +
                    '}';
        }
    }
}