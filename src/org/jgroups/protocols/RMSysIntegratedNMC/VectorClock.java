package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

public class VectorClock implements SizeStreamable {
    private MessageId lastBroadcast;
    private Map<Address, MessageId> ackRecord;

    public static VectorClock copyVectorClock(VectorClock clock) {
        return new VectorClock(clock.lastBroadcast, clock.ackRecord);
    }

    public VectorClock() {
        lastBroadcast = null;
        ackRecord = new HashMap<Address, MessageId>();
    }

    public VectorClock(MessageId lastBroadcast, Map<Address, MessageId> ackRecord) {
        this.lastBroadcast = lastBroadcast;
        this.ackRecord = ackRecord;
    }

    public MessageId getLastBroadcast() {
        return lastBroadcast;
    }

    public void setLastBroadcast(MessageId lastBroadcast) {
        this.lastBroadcast = lastBroadcast;
    }

    public Map<Address, MessageId> getAckRecord() {
        return ackRecord;
    }

    public void setAckRecord(Map<Address, MessageId> ackRecord) {
        this.ackRecord = ackRecord;
    }

    @Override
    public int size() {
        return lastBroadcast == null ? 0 : lastBroadcast.size() + getAckRecordSize();
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeStreamable(lastBroadcast, out);
        writeAckRecord(out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        lastBroadcast = (MessageId) Util.readStreamable(MessageId.class, in);
        ackRecord = readAckRecord(in);
    }

    @Override
    public String toString() {
        return "VectorClock{" +
                "lastBroadcast=" + lastBroadcast +
                ", ackRecord=" + ackRecord +
                '}';
    }

    private void writeAckRecord(DataOutput out) throws Exception {
        if (ackRecord == null) {
            out.writeShort(-1);
            return;
        }

        out.writeShort(ackRecord.size());
        for (Map.Entry<Address, MessageId> entry : ackRecord.entrySet()) {
            Util.writeAddress(entry.getKey(), out);
            Util.writeStreamable(entry.getValue(), out);
        }
    }

    private Map<Address, MessageId> readAckRecord(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;

        Map<Address, MessageId> ackRecord = new HashMap<Address, MessageId>();
        for (int i = 0; i < length; i++)
            ackRecord.put(Util.readAddress(in), (MessageId) Util.readStreamable(MessageId.class, in));
        return ackRecord;
    }

    private int getAckRecordSize() {
        int size = 0;
        for (Map.Entry<Address, MessageId> entry : ackRecord.entrySet()) {
            size += Util.size(entry.getKey()) + entry.getValue().size();
        }
        return size;
    }
}