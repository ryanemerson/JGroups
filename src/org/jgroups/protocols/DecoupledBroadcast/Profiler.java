package org.jgroups.protocols.DecoupledBroadcast;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class to record profiling information about the decoupled protocol
 *
 * @author ryan
 * @since 4.0
 */
public class Profiler {

    private static enum Counter {
        REQUESTS_RECEIVED,
        TOTAL_ORDER_MSGS,
        ORDERS_RECEIVED,
    }

    private final EnumMap<Counter, AtomicInteger> counters = new EnumMap<Counter, AtomicInteger>(Counter.class);
    private final boolean profileEnabled;

    public Profiler(boolean profileEnabled) {
        this.profileEnabled = profileEnabled;

        if (profileEnabled) {
            initialiseEnum(Counter.class, counters);
        }
    }

    public void requestsReceived(int numberOfRequests) {
        if (!profileEnabled)
            return;

        counters.get(Counter.REQUESTS_RECEIVED).addAndGet(numberOfRequests);
    }

    public void requestReceived() {
        requestsReceived(1);
    }

    public void totalOrderSent() {
        if (!profileEnabled)
            return;

        counters.get(Counter.TOTAL_ORDER_MSGS).incrementAndGet();
    }

    public void orderReceived() {
        if (!profileEnabled)
            return;

        counters.get(Counter.ORDERS_RECEIVED).incrementAndGet();
    }

    private int calculateAverageBundleSize() {
        return (int) Math.round(counters.get(Counter.REQUESTS_RECEIVED).doubleValue() / counters.get(Counter.TOTAL_ORDER_MSGS).doubleValue());
    }

    private <T extends Enum<T>> void initialiseEnum(Class<T> enumType, EnumMap<T, AtomicInteger> map) {
        for (T constant : enumType.getEnumConstants())
            map.put(constant, new AtomicInteger());
    }

    @Override
    public String toString() {
        return "Profiler{" +
                "\nCounters=" + counters +
                "\nAverage Bundle Size=" + calculateAverageBundleSize() +
                '}';
    }
}