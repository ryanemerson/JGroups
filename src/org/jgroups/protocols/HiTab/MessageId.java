package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class MessageId implements SizeStreamable {
    private long timestamp;
    private Address originator;
    private long sequence;

    public MessageId() {
    }

    public MessageId(long timestamp, Address originator, long sequence) {
        if(originator == null)
            throw new IllegalArgumentException("Sender cannot be null");

        this.timestamp = timestamp;
        this.originator = originator;
        this.sequence = sequence;
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

    public long getSequence() {
        return sequence;
    }

    @Override
    public int size() {
        return Util.size(timestamp) + Util.size(originator) + Util.size(sequence);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeLong(timestamp);
        Util.writeAddress(originator, out);
        out.writeLong(sequence);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        timestamp = in.readLong();
        originator = Util.readAddress(in);
        sequence = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageId messageId = (MessageId) o;

        if (sequence != messageId.sequence) return false;
        if (timestamp != messageId.timestamp) return false;
        if (!originator.equals(messageId.originator)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + originator.hashCode();
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