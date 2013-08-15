package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class HiTabHeader extends RMCastHeader {

    public static final byte BROADCAST = 1;
    public static final byte RETRANSMISSION = 2;
    public static final byte PLACEHOLDER_REQUEST = 3;
    public static final byte SEQUENCE_REQUEST = 4;

    private byte type = 0;
    private int capD = -1;
    private int capS = -1;
    private int xMax = -1;
    private Address ackInformer = null;
    private List<MessageId> ackList = null;

    public HiTabHeader() {
    }

    public static HiTabHeader createPlaceholder(MessageId id, Address ackInformer) {
        return new HiTabHeader(id, PLACEHOLDER_REQUEST, -1, -1, -1, ackInformer, null);
    }

    public static HiTabHeader createSequenceRequest(Address originator, long sequence) {
        MessageId id = new MessageId(-1, originator, sequence);
        return new HiTabHeader(id, SEQUENCE_REQUEST, -1, -1, -1, null, null);
    }

    public HiTabHeader(MessageId id, byte type, int capD, int capS, int xMax, Address ackInformer, List<MessageId> ackList) {
        this(id, null, -1, -1, type, capD, capS, xMax, ackInformer, ackList);
    }

    public HiTabHeader(MessageId id, Address disseminator, int copy, int copyTotal, byte type, int capD, int capS, int xMax, Address ackInformer, List<MessageId> ackList) {
        super(id, disseminator, copy, copyTotal);
        this.type = type;
        this.capD = capD;
        this.capS = capS;
        this.xMax = xMax;
        this.ackInformer = ackInformer;

        if (ackList == null)
            this.ackList = new ArrayList<MessageId>();
        else
            this.ackList = ackList;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public int getCapD() {
        return capD;
    }

    public void setCapD(int capD) {
        this.capD = capD;
    }

    public int getCapS() {
        return capS;
    }

    public void setCapS(int capS) {
        this.capS = capS;
    }

    public int getXMax() {
        return xMax;
    }

    public void setXMax(int xMax) {
        this.xMax = xMax;
    }

    public Address getAckInformer() {
        return ackInformer;
    }

    public void setAckInformer(Address ackInformer) {
        this.ackInformer = ackInformer;
    }

    public List<MessageId> getAckList() {
        return ackList;
    }

    public void setAckList(List<MessageId> ackList) {
        this.ackList = ackList;
    }

    @Override
    public int size() {
        return super.size() + (2 * Global.INT_SIZE) + Util.size(ackInformer) + ackListSize(ackList);
    }

    private int ackListSize(Collection<MessageId> ackList) {
        int size = 0;
        if(ackList != null && !ackList.isEmpty()) {
            for(MessageId id : ackList) {
                size += id.size();
            }
        }
        return size;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        out.writeByte(type);
        out.writeInt(capD);
        out.writeInt(capS);
        out.writeInt(xMax);
        Util.writeAddress(ackInformer, out);
        writeAckList(ackList, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        type = in.readByte();
        capD = in.readInt();
        capS = in.readInt();
        xMax = in.readInt();
        ackInformer = Util.readAddress(in);
        ackList = readAckList(in);
    }

    @Override
    public String toString() {
        return "HiTabHeader{" +
                "type=" + type2String(type) +
                ", capD=" + capD +
                ", capS=" + capS +
                ", xMax=" + xMax +
                ", ackInformer=" + ackInformer +
                ", ackList=" + ackList +
                "} " + super.toString();
    }

    public static String type2String(int t) {
        switch(t) {
            case BROADCAST:	                return "BROADCAST";
            case RETRANSMISSION:            return "RETRANSMISSION";
            case PLACEHOLDER_REQUEST:       return "PLACEHOLDER_REQUEST";
            case SEQUENCE_REQUEST:          return "SEQUENCE_REQUEST";
            default:                        return "UNDEFINED(" + t + ")";
        }
    }

    private void writeAckList(List<MessageId> ackList, DataOutput out) throws Exception {
        if(ackList == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(ackList.size());
        for(MessageId id : ackList) {
            id.writeTo(out);
        }
    }

    private List<MessageId> readAckList(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;

        List<MessageId> ackList = new ArrayList<MessageId>();
        MessageId id;
        for (int i = 0; i < length; i++) {
            id = new MessageId();
            id.readFrom(in);
        }
        return ackList;
    }
}