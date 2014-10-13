package org.jgroups.tests.probing_validation;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A Message Header used for messages sent between the master and slaves.
 *
 * @author a7109534
 * @since 4.0
 */
public class MasterHeader extends Header {

    public static final byte START_PROBING = 1;
    public static final byte PAST_LATENCIES = 2;
    public static final byte PRESENT_LATENCIES = 3;

    private byte type = 0;
    private int timePeriod = -1;
    private Collection<Address> destinations = new ArrayList<Address>();
    private List<LatencyTime> latencyTimes = new ArrayList<LatencyTime>();

    public static MasterHeader startProbingMessage(int timePeriod, Collection<Address> destinations) {
        return new MasterHeader(START_PROBING, timePeriod, destinations, new ArrayList<LatencyTime>());
    }

    public static MasterHeader probingComplete(byte type, int timePeriod, List<LatencyTime> latencies) {
        return new MasterHeader(type, timePeriod, null, latencies);
    }

    public MasterHeader() {
    }

    public MasterHeader(byte type, int timePeriod, Collection<Address> destinations, List<LatencyTime> latencyTimes) {
        this.type = type;
        this.timePeriod = timePeriod;
        this.destinations = destinations;
        this.latencyTimes = latencyTimes;
    }

    public byte getType() {
        return type;
    }

    public int getTimePeriod() {
        return timePeriod;
    }

    public Collection<Address> getDestinations() {
        return destinations;
    }

    public List<LatencyTime> getLatencyTimes() {
        return latencyTimes;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + Global.INT_SIZE + (int) Util.size(destinations) + getLatenciesSize();
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        out.writeInt(timePeriod);
        Util.writeAddresses(destinations, out);
        writeLatencyTimes(out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        timePeriod = in.readInt();
        destinations = (Collection<Address>) Util.readAddresses(in, ArrayList.class);
        latencyTimes = readLatencyTimes(in);
    }

    @Override
    public String toString() {
        return "MasterHeader{" +
                "type=" + type2String(type) +
                ", timePeriod=" + timePeriod +
                ", destinations=" + destinations +
                ", latencyTimes=" + latencyTimes +
                '}';
    }

    public static String type2String(byte t) {
        switch (t) {
            case START_PROBING:
                return "START_PROBING";
            case PRESENT_LATENCIES:
                return "PRESENT_LATENCIES";
            case PAST_LATENCIES:
                return "PAST_LATENCIES";
            default:
                return "UNDEFINED(" + t + ")";
        }
    }

    private void writeLatencyTimes(DataOutput out) throws Exception {
        if (latencyTimes == null) {
            out.writeShort(-1);
            return;
        }

        out.writeShort(latencyTimes.size());
        for (LatencyTime latency : latencyTimes)
            Util.writeStreamable(latency, out);
    }

    private List<LatencyTime> readLatencyTimes(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;

        List<LatencyTime> latencies = new ArrayList<LatencyTime>();
        for (int i = 0; i < length; i++)
            latencies.add((LatencyTime) Util.readStreamable(LatencyTime.class, in));
        return latencies;
    }

    private int getLatenciesSize() {
        int total = 0;
        for (LatencyTime latency : latencyTimes)
            total += latency.size();
        return total;
    }
}