package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Message Id used by all RMSys messages
 *
 * @author ryan
 * @since 4.0
 */
public class MessageId implements SizeStreamable, Comparable<MessageId> {
    private volatile long timestamp;
    private volatile Address originator;
    private volatile long sequence;

    public MessageId() {
    }

    public MessageId(long timestamp, Address originator, long sequence) {
        if (originator == null)
            throw new IllegalArgumentException("Sender cannot be null");

        this.timestamp = timestamp;
        this.originator = originator;
        this.sequence = sequence;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Address getOriginator() {
        return originator;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public int size() {
        return Bits.size(timestamp) + Util.size(originator) + Bits.size(sequence);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Bits.writeLong(timestamp, out);
        Util.writeAddress(originator, out);
        Bits.writeLong(sequence, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        timestamp = Bits.readLong(in);
        originator = Util.readAddress(in);
        sequence = Bits.readLong(in);
    }

    @Override
    public int compareTo(MessageId other) {
        if (other == null) return 1;

        if (this.equals(other))
            return 0;

        if (originator.equals(other.originator))
            if (timestamp == other.timestamp && timestamp == -1)
                return Long.signum(sequence - other.sequence);

        if (timestamp < other.timestamp)
            return -1;
        else if(timestamp > other.timestamp)
            return 1;
        else
            return originator.compareTo(other.originator);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageId messageId = (MessageId) o;
        if (sequence != messageId.sequence)
            return false;

        if (timestamp != messageId.timestamp)
            return false;

        if (originator != null ? !originator.equals(messageId.originator) : messageId.originator != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (originator != null ? originator.hashCode() : 0);
        result = 31 * result + (int) (sequence ^ (sequence >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MessageId{" +
                "timestamp=" + timestamp +
                ", originator=" + originator +
                ", sequence=" + sequence +
                '}';
    }
}