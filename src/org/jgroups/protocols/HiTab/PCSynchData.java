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
public class PCSynchData implements SizeStreamable {
    private Address slave = null; // Address of the slave node trying to synch
    private Address master = null; // Address of the master node
    private long requestTime = -1; // Time that the original request was sent
    private long responseTime = -1; // Sending time of a response message

    public PCSynchData() {
    }

    PCSynchData(Address slave, long timeSent) {
        this.slave = slave;
        this.requestTime = timeSent;
    }

    PCSynchData(Address slave, Address master, long requestTime, long responseTime) {
        this(slave, requestTime);
        this.master = master;
        this.responseTime = responseTime;
    }

    PCSynchData(Address master, long responseTime, PCSynchData request) {
        this(request.getSlave(), master, request.getRequestTime(), responseTime);
    }

    public Address getSlave() {
        return slave;
    }

    public Address getMaster() {
        return master;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public long getResponseTime() {
        return responseTime;
    }

    @Override
    public int size() {
        return Util.size(slave) + Util.size(master) + Util.size(requestTime) + Util.size(responseTime);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeAddress(slave, out);
        Util.writeAddress(master, out);
        out.writeLong(requestTime);
        out.writeLong(responseTime);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        slave = Util.readAddress(in);
        master = Util.readAddress(in);
        requestTime = in.readLong();
        responseTime = in.readLong();
    }

    @Override
    public String toString() {
        return "PCSynchData{" +
                "slave=" + slave +
                ", master=" + master +
                ", requestTime=" + requestTime +
                ", responseTime=" + responseTime +
                '}';
    }
}