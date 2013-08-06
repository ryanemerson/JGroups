package org.jgroups.protocols.HiTab;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
/**
 A class to represent latencies encountered by probes across the network
 */
final public class LatencyTime implements Comparable<LatencyTime> {
    final private long latency; // Nanoseconds
    final private long freshness; // Nanoseconds

    public LatencyTime(long latency, long freshness) {
        this.latency = latency;
        this.freshness = freshness;
    }

    public long getLatency() {
        return latency;
    }

    public long getFreshness() {
        return freshness;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LatencyTime that = (LatencyTime) o;

        if (freshness != that.freshness) return false;
        if (latency != that.latency) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (latency ^ (latency >>> 32));
        result = 31 * result + (int) (freshness ^ (freshness >>> 32));
        return result;
    }

    @Override
    public int compareTo(LatencyTime other) {
        if (this.equals(other))
            return 0;
        else if(latency > other.latency)
            return 1;
        else
            return -1;
    }
}