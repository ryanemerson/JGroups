package org.jgroups.protocols.HiTab;

import org.jgroups.Address;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
final public class ProbeRecord {
    private final Address destination;
    private final long timeSent; // Nano Time
    private volatile boolean isLost;
    private volatile boolean received;

    public ProbeRecord(Address destination, long timeSent, boolean isLost, boolean received) {
        this.destination = destination;
        this.timeSent = timeSent;
        this.isLost = isLost;
        this.received = received;
    }

    public Address getDestination() {
        return destination;
    }

    public long getTimeSent() {
        return timeSent;
    }

    public boolean isLost() {
        return isLost;
    }

    public void setLost(boolean lost) {
        isLost = lost;
    }

    public boolean isReceived() {
        return received;
    }

    public void setReceived(boolean received) {
        this.received = received;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProbeRecord that = (ProbeRecord) o;

        if (timeSent != that.timeSent) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (timeSent ^ (timeSent >>> 32));
    }
}