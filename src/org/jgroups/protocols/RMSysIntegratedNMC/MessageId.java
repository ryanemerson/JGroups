package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
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
    private long timestamp;
    private Address originator;

    public MessageId() {
    }

    public MessageId(long timestamp, Address originator, long sequence) {
        if (originator == null)
            throw new IllegalArgumentException("Sender cannot be null");

        this.timestamp = timestamp;
        this.originator = originator;
    }

    public MessageId(long timestamp, Address originator) {
        this(timestamp, originator, -1);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Address getOriginator() {
        return originator;
    }


    @Override
    public int size() {
        return Util.size(timestamp) + Util.size(originator);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeLong(timestamp);
        Util.writeAddress(originator, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        timestamp = in.readLong();
        originator = Util.readAddress(in);
    }

    @Override
    public int compareTo(MessageId other) {
        if (other == null) return 1;

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

        if (timestamp != messageId.timestamp) return false;
        if (originator != null ? !originator.equals(messageId.originator) : messageId.originator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (originator != null ? originator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MessageId{" +
                "timestamp=" + timestamp +
                ", originator=" + originator +
                '}';
    }
}