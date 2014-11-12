package org.jgroups.protocols.aramis;

import java.text.DecimalFormat;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class to profile the performance of Aramis and Base.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class Profiler {

    private static enum Counter {
        PROBES_RECORDED,
        MESSAGES_SENT,
        MESSAGES_RECEIVED,
        MESSAGES_DELIVERED,
        MESSAGES_REJECTED,
        MESSAGES_TIMEDOUT,
        EMPTY_PROBE_MESSAGES_SENT,
        EMPTY_PROBE_MESSAGES_RECEIVED,
        EMPTY_ACK_MESSAGES_SENT,
        EMPTY_ACK_MESSAGES_RECEIVED,
        FIRST_COPY_NOT_0,
        COPY_GREATER_THAN_0_SENT,
        MESSAGES_DISSEMINATED, // Actually records the number of copies disseminated
        ACK_PLACEHOLDERS, // Number of placeholders created because of acks
        SEQ_PLACEHOLDERS // Number of placeholders created based upon missing sequence numbers
    }

    private static enum ProbeLatency {
        SMALLEST,
        LARGEST,
        LARGEST_XMAX,
        SMALLEST_XMAX,
    }

    private static enum DeliveryLatency {
        SMALLEST,
        LARGEST,
        SMALLEST_DELIVERY_DELAY,
        LARGEST_DELIVERY_DELAY,
    }

    private final EnumMap<Counter, AtomicInteger> counters = new EnumMap<Counter, AtomicInteger>(Counter.class);
    private final EnumMap<ProbeLatency, AtomicLong> probeLatencies = new EnumMap<ProbeLatency, AtomicLong>(ProbeLatency.class);
    private final EnumMap<DeliveryLatency, AtomicLong> deliveryLatencies = new EnumMap<DeliveryLatency, AtomicLong>(DeliveryLatency.class);
    private final AtomicLong averageProbeLatency = new AtomicLong();
    private final boolean profileEnabled;
    private boolean longNMC = false;

    private final AtomicLong deliveryDelayTotal = new AtomicLong();
    private final AtomicLong deliveryDelayCount = new AtomicLong();
    private final AtomicLong deliveryLatencyTotal = new AtomicLong();
    private final AtomicLong deliveryLatencyCount = new AtomicLong();

    public Profiler(boolean profileEnabled) {
        this.profileEnabled = profileEnabled;

        if (profileEnabled) {
            initialiseEnum(Counter.class, counters);
            initialiseEnumWithLong(ProbeLatency.class, probeLatencies);
            initialiseEnumWithLong(DeliveryLatency.class, deliveryLatencies);
        }
    }

    public void longNMCUsed() {
        longNMC = true;
    }

    public void addDeliveryDelay(long delay) {
        if (!profileEnabled)
            return;

        AtomicLong smallest = deliveryLatencies.get(DeliveryLatency.SMALLEST_DELIVERY_DELAY);
        if (smallest.longValue() == 0 || delay < smallest.longValue())
            smallest.set(delay);

        AtomicLong largest = deliveryLatencies.get(DeliveryLatency.LARGEST_DELIVERY_DELAY);
        if (delay > largest.longValue())
            largest.set(delay);

        // Used for calculating average
        deliveryDelayTotal.addAndGet(delay);
        deliveryDelayCount.incrementAndGet();
    }

    public void addDeliveryLatency(long latency) {
        addLatency(latency, true);

        // Used for calculating average
        deliveryLatencyTotal.addAndGet(latency);
        deliveryLatencyCount.incrementAndGet();
    }

    public void addProbeLatency(long latency) {
        addLatency(latency, false);
    }

    public void addLocalXmax(long xMax) {
        addXMax(xMax);
    }

    public void probeReceieved() {
        if (!profileEnabled)
            return;

        counters.get(Counter.PROBES_RECORDED).incrementAndGet();
    }

    public void messageSent() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_SENT).incrementAndGet();
    }

    public void copyGreaterThanZero() {
        if (!profileEnabled)
            return;

        counters.get(Counter.COPY_GREATER_THAN_0_SENT).incrementAndGet();
    }

    public void messageReceived(boolean firstCopyAbsent) {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_RECEIVED).incrementAndGet();
        if (firstCopyAbsent)
            counters.get(Counter.FIRST_COPY_NOT_0).incrementAndGet();
    }

    public void emptyProbeMessageReceived() {
        if (!profileEnabled)
            return;

        counters.get(Counter.EMPTY_PROBE_MESSAGES_RECEIVED).incrementAndGet();
    }

    public void emptyProbeMessageSent() {
        if (!profileEnabled)
            return;

        counters.get(Counter.EMPTY_PROBE_MESSAGES_SENT).incrementAndGet();
    }

    public void emptyAckMessageReceived() {
        if (!profileEnabled)
            return;

        counters.get(Counter.EMPTY_ACK_MESSAGES_RECEIVED).incrementAndGet();
    }

    public void emptyAckMessageSent() {
        if (!profileEnabled)
            return;

        counters.get(Counter.EMPTY_ACK_MESSAGES_SENT).incrementAndGet();
    }

    public void messageRejected() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_REJECTED).incrementAndGet();
    }

    public void messageTimedOut() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_TIMEDOUT).incrementAndGet();
    }

    public void messageDisseminated() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_DISSEMINATED).incrementAndGet();
    }

    public void ackPlaceholderCreated() {
        if (!profileEnabled)
            return;

        counters.get(Counter.ACK_PLACEHOLDERS).incrementAndGet();
    }

    public void seqPlaceholderCreated() {
        if (!profileEnabled)
            return;

        counters.get(Counter.SEQ_PLACEHOLDERS).incrementAndGet();
    }

    public void messageDelivered() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_DELIVERED).incrementAndGet();
    }

    private void addLatency(long latency, boolean deliveryLatency) {
        if (!profileEnabled)
            return;

        checkLargestLatency(latency, deliveryLatency);
        checkSmallestLatency(latency, deliveryLatency);

        if (!deliveryLatency)
            calculateAverageProbeLatency(latency);
    }

    private void checkLargestLatency(long latency, boolean deliveryLatency) {
        AtomicLong largest = deliveryLatency ? deliveryLatencies.get(DeliveryLatency.LARGEST) :
                probeLatencies.get(ProbeLatency.LARGEST);

        if (latency > largest.longValue())
            largest.set(latency);
    }

    private void checkSmallestLatency(long latency, boolean deliveryLatency) {
        AtomicLong smallest = deliveryLatency ? deliveryLatencies.get(DeliveryLatency.SMALLEST) :
                probeLatencies.get(ProbeLatency.SMALLEST);

        if (smallest.longValue() == 0 || latency < smallest.longValue())
            smallest.set(latency);
    }

    // We don't count the total as probe latencies are stored in nanoseconds, and we don't want the MAX LONG size to be
    // reached on large experiments.
    private void calculateAverageProbeLatency(long latency) {
        double average = Double.longBitsToDouble(averageProbeLatency.longValue());
        average = (average + latency) / 2;
        long longAverage = Double.doubleToLongBits(average); // No AtomicDouble class, so long used instead
        averageProbeLatency.set(longAverage);
    }

    private void addXMax(long xMax) {
        if (!profileEnabled)
            return;

        checkLargestXmax(xMax);
        checkSmallestXmax(xMax);
    }

    private void checkLargestXmax(long newXMax) {
        AtomicLong xMax = probeLatencies.get(ProbeLatency.LARGEST_XMAX);
        if (newXMax > xMax.longValue())
            xMax.set(newXMax);
    }

    private void checkSmallestXmax(long newXMax) {
        AtomicLong xMax = probeLatencies.get(ProbeLatency.SMALLEST_XMAX);
        if (xMax.longValue() == 0 || newXMax < xMax.longValue())
            xMax.set(newXMax);
    }

    private <T extends Enum<T>> void initialiseEnum(Class<T> enumType, EnumMap<T, AtomicInteger> map) {
        for (T constant : enumType.getEnumConstants())
            map.put(constant, new AtomicInteger());
    }

    private <T extends Enum<T>> void initialiseEnumWithLong(Class<T> enumType, EnumMap<T, AtomicLong> map) {
        for (T constant : enumType.getEnumConstants())
            map.put(constant, new AtomicLong());
    }

    @Override
    public String toString() {
        if (longNMC)
            return nanoOutput();

        return milliOutput();
    }

    private String milliOutput() {
        DecimalFormat df =  new DecimalFormat("##.00");
        String averageProbe = df.format(Double.longBitsToDouble(averageProbeLatency.longValue()));
        String averageDeliveryLatency = df.format(Double.longBitsToDouble(deliveryLatencyTotal.get()) / Double.longBitsToDouble(deliveryLatencyCount.get()));
        String averageDeliveryDelay = df.format(Double.longBitsToDouble(deliveryDelayTotal.get()) / Double.longBitsToDouble(deliveryDelayCount.get()));
        return "Profiler{" +
                "\nCounters=" + counters +
                ",\nProbe Latencies=" + probeLatencies +
                ",\nAverage ProbeLatency=" + averageProbe + "ms" +
                ",\nDelivery Latencies=" + deliveryLatencies +
                ",\nAverage DeliveryLatency=" + averageDeliveryLatency + "ms" +
                ",\nAverage DeliveryDelay=" + averageDeliveryDelay + "ms" +
                '}';
    }

    private String nanoOutput() {
        DecimalFormat df =  new DecimalFormat("##.00");
        String averageDeliveryLatency = df.format(Double.longBitsToDouble(deliveryLatencyTotal.get()) / Double.longBitsToDouble(deliveryLatencyCount.get()));
        String averageDeliveryDelay = df.format(Double.longBitsToDouble(deliveryDelayTotal.get()) / Double.longBitsToDouble(deliveryDelayCount.get()));
        return "Profiler{" +
                "\nCounters=" + counters +
                ",\nProbe Latencies=" + enumNanoOutput(probeLatencies) +
                ",\nAverage ProbeLatency=" + df.format(Double.longBitsToDouble(averageProbeLatency.longValue()) / 1e+6) + "ms" +
                ",\nDelivery Latencies=" + deliveryLatencies +
                ",\nAverage DeliveryLatency=" + averageDeliveryLatency + "ms" +
                ",\nAverage DeliveryDelay=" +  averageDeliveryDelay + "ms" +
                '}';
    }

    private <T extends Enum<T>> String enumNanoOutput(EnumMap<T, AtomicLong> enumMap) {
        DecimalFormat df =  new DecimalFormat("##.00");
        String output = "{";
        for (Map.Entry<T, AtomicLong> entry : enumMap.entrySet())
            output += entry.getKey() + "=" + df.format(entry.getValue().longValue() / 1e+6) + ", ";
        return output.substring(0, output.length() - 2) + "}";
    }
}