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
public class ProbeHeader extends Header {
    public static final byte PROBE_REQ=1;
    public static final byte PROBE_RSP=2;

    private byte type = 0;
    private ProbeData data = null;
    private String clusterName = null;

    public ProbeHeader() {
    }

    public ProbeHeader(byte type) {
        this.type = type;
    }

    public ProbeHeader(byte type, ProbeData data) {
        this(type);
        this.data = data;
    }

    public ProbeHeader(byte type, ProbeData data, String clusterName) {
        this(type, data);
        this.clusterName = clusterName;
    }

    public byte getType() {
        return type;
    }

    public ProbeData getData() {
        return data;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Probe: type=" + typeToString(type));
        if(data != null)
            sb.append(", arg=" + data);
        if(clusterName != null)
            sb.append(", cluster=").append(clusterName);
        sb.append(']');
        return sb.toString();
    }

    static String typeToString(byte type) {
        switch(type) {
            case PROBE_REQ: return "PROBE_REQ";
            case PROBE_RSP: return "PROBE_RSP";
            default:        return "<unknown type(" + type + ")>";
        }
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

    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        Util.writeStreamable(data, out);
        Util.writeString(clusterName, out);
    }

    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        data = (ProbeData) Util.readStreamable(ProbeData.class, in);
        clusterName = Util.readString(in);
    }
}