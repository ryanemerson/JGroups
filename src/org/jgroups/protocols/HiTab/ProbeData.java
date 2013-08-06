package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class ProbeData extends PingData{
    protected int xMax = 0; // xMax - The largest latency encountered by the sending node
    protected long timeSent = -1L; // The System.nanoTime() at which the probe was created

    public ProbeData() {
    }

    public ProbeData(Address sender, View view, ViewId viewId, String logicalName,
                     Collection<PhysicalAddress> physicalAddresses, int xMax, long timeSent) {
        super(sender, view, viewId, false, logicalName, physicalAddresses);
        this.xMax = xMax;
        this.timeSent = timeSent;
    }

    public ProbeData(Address sender, View view, String logicalName,
                     Collection<PhysicalAddress> physicalAddresses, int xMax, long timeSent) {
        super(sender, view, false, logicalName, physicalAddresses);
        this.xMax = xMax;
        this.timeSent = timeSent;
    }

    public int getXMax() {
        return xMax;
    }

    public void setXMax(int xMax) {
        this.xMax = xMax;
    }

    public long getTimeSent() {
        return timeSent;
    }

    public void setTimeSent(long timeSent) {
        this.timeSent = timeSent;
    }

    @Override
    public String toString() {
        return super.toString() + ", xMax=" + xMax + ", timeSent=" + timeSent;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        out.writeInt(xMax);
        out.writeLong(timeSent);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        xMax = in.readInt();
        timeSent = in.readLong();
    }

    @Override
    public int size() {
        return super.size() + Global.INT_SIZE + Util.size(timeSent);
    }
}