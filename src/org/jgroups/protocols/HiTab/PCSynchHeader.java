package org.jgroups.protocols.HiTab;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class PCSynchHeader extends Header {

    public static final byte SYNCH_REQ = 1;
    public static final byte SYNCH_RSP = 2;

    private byte type = 0;
    private PCSynchData data = null;
    private String clusterName = null;

    public PCSynchHeader() {
    }

    public PCSynchHeader(byte type, PCSynchData data) {
        this.type = type;
        this.data = data;
    }

    public PCSynchHeader(byte type, PCSynchData data, String clusterName) {
        this(type, data);
        this.clusterName = clusterName;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public PCSynchData getData() {
        return data;
    }

    public void setData(PCSynchData data) {
        this.data = data;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public int size() {
        int retval = Global.BYTE_SIZE * 3; // type, data presence and cluster_name presence
        if(data != null)
            retval += data.size();
        if(clusterName != null)
            retval += clusterName.length() + 2;
        return retval;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        Util.writeStreamable(data, out);
        Util.writeString(clusterName, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        data = (PCSynchData) Util.readStreamable(PCSynchData.class, in);
        clusterName = Util.readString(in);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[PCSynchMsg: type=" + typeToString(type));
        if(data != null)
            sb.append(", arg=" + data);
        if(clusterName != null)
            sb.append(", cluster=").append(clusterName);
        sb.append(']');
        return sb.toString();
    }

    static String typeToString(byte type) {
        switch(type) {
            case SYNCH_REQ: return "SYNCH_REQ";
            case SYNCH_RSP: return "SYNCH_RSP";
            default:        return "<unknown type(" + type + ")>";
        }
    }
}