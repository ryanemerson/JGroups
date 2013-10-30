package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.Global;
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
    private Address originator = null; // Originator of an ordering request
    private int sequence = -1; // Sequence local to the requesting node

    public MessageId() {
    }

    public MessageId(Address originator, int sequence) {
        this.originator = originator;
        this.sequence = sequence;
    }

    public Address getOriginator() {
        return originator;
    }

    public int getSequence() {
        return sequence;
    }

    @Override
    public int size() {
        return Util.size(originator) + Global.INT_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(originator, out);
        out.writeInt(sequence);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        originator = Util.readAddress(in);
        sequence = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageId messageId = (MessageId) o;

        if (sequence != messageId.sequence) return false;
        if (originator != null ? !originator.equals(messageId.originator) : messageId.originator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sequence;
        result = 31 * result + (originator != null ? originator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MessageId{" +
                "sequence=" + sequence +
                ", originator=" + originator +
                '}';
    }
}