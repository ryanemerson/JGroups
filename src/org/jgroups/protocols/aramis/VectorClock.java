package org.jgroups.protocols.aramis;

import org.jgroups.Address;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Class used to provide vector clocks in Aramis and Base.
 *
 * @author ryan
 * @since 4.0
 */
public class VectorClock implements SizeStreamable {
    private MessageId lastBroadcast;
    private Map<Address, MessageId> messagesReceived;

    public static VectorClock copyVectorClock(VectorClock clock) {
        return new VectorClock(clock.lastBroadcast, clock.messagesReceived);
    }

    public VectorClock() {
        lastBroadcast = null;
        messagesReceived = new HashMap<Address, MessageId>();
    }

    public VectorClock(MessageId lastBroadcast, Map<Address, MessageId> messagesReceived) {
        this.lastBroadcast = lastBroadcast;
        this.messagesReceived = messagesReceived;
    }

    public MessageId getLastBroadcast() {
        return lastBroadcast;
    }

    public void setLastBroadcast(MessageId lastBroadcast) {
        this.lastBroadcast = lastBroadcast;
    }

    public Map<Address, MessageId> getMessagesReceived() {
        return messagesReceived;
    }

    public void setMessagesReceived(Map<Address, MessageId> messagesReceived) {
        this.messagesReceived = messagesReceived;
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
        messagesReceived = readAckRecord(in);
    }

    @Override
    public String toString() {
        return "VectorClock{" +
                "lastBroadcast=" + lastBroadcast +
                ", messagesReceived=" + messagesReceived +
                '}';
    }

    private void writeAckRecord(DataOutput out) throws Exception {
        if (messagesReceived == null) {
            out.writeShort(-1);
            return;
        }

        out.writeShort(messagesReceived.size());
        for (Map.Entry<Address, MessageId> entry : messagesReceived.entrySet()) {
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
        for (Map.Entry<Address, MessageId> entry : messagesReceived.entrySet()) {
            size += Util.size(entry.getKey()) + entry.getValue().size();
        }
        return size;
    }
}