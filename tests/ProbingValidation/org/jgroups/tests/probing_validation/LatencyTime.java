package org.jgroups.tests.probing_validation;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * A class that stores recorded latency data
 *
 * @author a7109534
 * @since 4.0
 */
public class LatencyTime implements Streamable {
    Address destination;
    long latency;
    long creationTime;

    public LatencyTime() {}

    public LatencyTime(Address destination, long latency) {
        this(destination, latency, System.nanoTime());
    }

    public LatencyTime(Address destination, long latency, long creationTime) {
        this.destination = destination;
        this.latency = latency;
        this.creationTime = creationTime;
    }

    public Address getDestination() {
        return destination;
    }

    public long getLatency() {
        return latency;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public int size() {
        return Util.size(destination) + Bits.size(latency) + Bits.size(creationTime);
    }

    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(destination, out);
        Bits.writeLong(latency, out);
        Bits.writeLong(creationTime, out);
    }

    public void readFrom(DataInput in) throws Exception {
        destination = Util.readAddress(in);
        latency = Bits.readLong(in);
        creationTime = Bits.readLong(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LatencyTime that = (LatencyTime) o;

        if (creationTime != that.creationTime) return false;
        if (latency != that.latency) return false;
        if (destination != null ? !destination.equals(that.destination) : that.destination != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = destination != null ? destination.hashCode() : 0;
        result = 31 * result + (int) (latency ^ (latency >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        return result;
    }
}