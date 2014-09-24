package org.jgroups.protocols.HiTab;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * // TODO: Document this
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class MessageRejectionHeader extends Header {
    public static final byte ABORT = 1;
    public static final byte REJECT = 2;

    private byte type = 0;
    private long timeTaken = 0;

    public MessageRejectionHeader() {
    }

    public MessageRejectionHeader(byte type) {
        this.type = type;
    }

    public MessageRejectionHeader(byte type, long rejectionTime) {
        this.type = type;
        this.timeTaken = rejectionTime;
    }

    public byte getType() {
        return type;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        Bits.writeLong(timeTaken, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        timeTaken = Bits.readLong(in);
    }

    @Override
    public String toString() {
        return "MessageRejectionHeader{" +
                "type=" + type +
                ", timeTaken=" + timeTaken +
                '}';
    }

    public static String type2String(int t) {
        switch(t) {
            case ABORT:	                return "ABORT";
            case REJECT:                return "REJECT";
            default:                    return "UNDEFINED(" + t + ")";
        }
    }
}
