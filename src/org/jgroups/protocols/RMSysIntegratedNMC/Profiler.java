package org.jgroups.protocols.RMSysIntegratedNMC;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class Profiler {

    private static enum Counter {
        PROBES_RECORDED,
        MESSAGES_RECEIVED,
        MESSAGES_DELIVERED,
        MESSAGES_REJECTED,
        MESSAGES_TIMEDOUT,
        EMPTY_PROBE_MESSAGES_SENT,
        EMPTY_PROBE_MESSAGES_RECEIVED,
        EMPTY_ACK_MESSAGES_SENT,
        EMPTY_ACK_MESSAGES_RECEIVED,
        FIRST_COPY_NOT_0,
        MESSAGES_DISSEMINATED // Actually records the number of copies disseminated
    }

    private static enum ProbeLatency {
        SMALLEST,
        LARGEST,
        LARGEST_XMAX,
        SMALLEST_XMAX,
        LARGEST_GLOBAL_XMAX,
        SMALLEST_GLOBAL_XMAX
    }

    private static enum DeliveryLatency {
        SMALLEST,
        LARGEST,
    }

    private final EnumMap<Counter, AtomicInteger> counters = new EnumMap<Counter, AtomicInteger>(Counter.class);
    private final EnumMap<ProbeLatency, AtomicInteger> probeLatencies = new EnumMap<ProbeLatency, AtomicInteger>(ProbeLatency.class);
    private final EnumMap<DeliveryLatency, AtomicInteger> deliveryLatencies = new EnumMap<DeliveryLatency, AtomicInteger>(DeliveryLatency.class);
    private final AtomicLong averageProbeLatency = new AtomicLong();
    private final AtomicLong averageDeliveryLatency = new AtomicLong();
    private final boolean profileEnabled;

    public Profiler(boolean profileEnabled) {
        this.profileEnabled = profileEnabled;

        if (profileEnabled) {
            initialiseEnum(Counter.class, counters);
            initialiseEnum(ProbeLatency.class, probeLatencies);
            initialiseEnum(DeliveryLatency.class, deliveryLatencies);
        }
    }

    public void addDeliveryLatency(int latency) {
        addLatency(latency, true);
    }

    public void addProbeLatency(int latency) {
        addLatency(latency, false);
    }

    public void addLocalXmax(int xMax) {
        addXMax(xMax, true);
    }

    public void addGlobalXmax(int xMax) {
        addXMax(xMax, false);
    }

    public void probeReceieved() {
        if (!profileEnabled)
            return;

        counters.get(Counter.PROBES_RECORDED).incrementAndGet();
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

    public void messageDelivered() {
        if (!profileEnabled)
            return;

        counters.get(Counter.MESSAGES_DELIVERED).incrementAndGet();
    }

    private void addLatency(int latency, boolean deliveryLatency) {
        if (!profileEnabled)
            return;

        checkLargestLatency(latency, deliveryLatency);
        checkSmallestLatency(latency, deliveryLatency);
        calculateAverageLatency(latency, deliveryLatency);
    }

    private void checkLargestLatency(int latency, boolean deliveryLatency) {
        AtomicInteger largest = deliveryLatency ? deliveryLatencies.get(DeliveryLatency.LARGEST) :
                probeLatencies.get(ProbeLatency.LARGEST);

        if (latency > largest.intValue())
            largest.set(latency);
    }

    private void checkSmallestLatency(int latency, boolean deliveryLatency) {
        AtomicInteger smallest = deliveryLatency ? deliveryLatencies.get(DeliveryLatency.SMALLEST) :
                probeLatencies.get(ProbeLatency.SMALLEST);

        if (smallest.intValue() == 0 || latency < smallest.intValue())
            smallest.set(latency);
    }

    private void calculateAverageLatency(int latency, boolean deliveryLatency) {
        AtomicLong averageLatency = deliveryLatency ? averageDeliveryLatency : averageProbeLatency;
        double average = Double.longBitsToDouble(averageLatency.longValue());
        average = (average + latency) / 2;
        long longAverage = Double.doubleToLongBits(average); // No AtomicDouble class, so long used instead
        averageLatency.set(longAverage);
    }

    private void addXMax(int xMax, boolean local) {
        if (!profileEnabled)
            return;

        checkLargestXmax(xMax, local);
        checkSmallestXmax(xMax, local);
    }

    private void checkLargestXmax(int newXMax, boolean local) {
        AtomicInteger xMax = local ? probeLatencies.get(ProbeLatency.LARGEST_XMAX) : probeLatencies.get(ProbeLatency.LARGEST_GLOBAL_XMAX);
        if (newXMax > xMax.intValue())
            xMax.set(newXMax);
    }

    private void checkSmallestXmax(int newXMax, boolean local) {
        AtomicInteger xMax = local ? probeLatencies.get(ProbeLatency.SMALLEST_XMAX) : probeLatencies.get(ProbeLatency.SMALLEST_GLOBAL_XMAX);
        if (xMax.intValue() == 0 || newXMax < xMax.intValue())
            xMax.set(newXMax);
    }

    private <T extends Enum<T>> void initialiseEnum(Class<T> enumType, EnumMap<T, AtomicInteger> map) {
        for (T constant : enumType.getEnumConstants())
            map.put(constant, new AtomicInteger());
    }

    @Override
    public String toString() {
        double averageProbe = Math.round(Double.longBitsToDouble(averageProbeLatency.longValue()) * 100.0) / 100.0;
        double averageDelivery = Math.round(Double.longBitsToDouble(averageDeliveryLatency.longValue()) * 100.0) / 100.0;
        return "Profiler{" +
                "\nCounters=" + counters +
                ",\nProbe Latencies=" + probeLatencies +
                ",\nAverage ProbeLatency=" + averageProbe + "ms" +
                ",\nDelivery Latencies=" + deliveryLatencies +
                ",\nAverage DeliveryLatency=" + averageDelivery + "ms" +
                '}';
    }
}