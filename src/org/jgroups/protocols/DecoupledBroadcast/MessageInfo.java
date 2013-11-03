package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Global;
import org.jgroups.ViewId;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class MessageInfo implements Comparable<MessageInfo>, SizeStreamable {
    private MessageId id = null;
    private long ordering = -1; // Sequence provided by the BOX, value created after TOA and before placed in the queue
    private ViewId viewId = null;
    private byte[] destinations = new byte[0];

    public MessageInfo() {
    }

    public MessageInfo(MessageId id, ViewId viewId, byte[] destinations) {
        this(id, -1, viewId, destinations);
    }

    public MessageInfo(MessageId id, long ordering, ViewId viewId, byte[] destinations) {
        this.id = id;
        this.ordering = ordering;
        this.viewId = viewId;
        this.destinations = destinations;
    }

    public MessageId getId() {
        return id;
    }

    public void setId(MessageId id) {
        this.id = id;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }

    public ViewId getViewId() {
        return viewId;
    }

    public void setViewId(ViewId viewId) {
        this.viewId = viewId;
    }

    public byte[] getDestinations() {
        return destinations;
    }

    public void setDestinations(byte[] destinations) {
        this.destinations = destinations;
    }

    @Override
    public int size() {
        return id.size() + Global.INT_SIZE + viewId.serializedSize() + Util.size(destinations);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        writeMessageId(id, out);
        Util.writeLong(ordering, out);
        Util.writeViewId(viewId, out);
        Util.writeByteBuffer(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = readMessageId(in);
        ordering = Util.readLong(in);
        viewId = Util.readViewId(in);
        destinations = Util.readByteBuffer(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageInfo that = (MessageInfo) o;

        if (!Arrays.equals(destinations, that.destinations)) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (viewId != null ? !viewId.equals(that.viewId) : that.viewId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (viewId != null ? viewId.hashCode() : 0);
        result = 31 * result + (destinations != null ? Arrays.hashCode(destinations) : 0);
        return result;
    }

    @Override
    public int compareTo(MessageInfo other) {
        if (this.equals(other))
            return 0;
        else if (ordering > other.ordering)
            return 1;
        else
            return -1;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "id=" + id +
                ", ordering=" + ordering +
                ", viewId=" + viewId +
                ", destinations=" + Arrays.toString(destinations) +
                '}';
    }

    private void writeMessageId(MessageId id, DataOutput out) throws Exception {
        if (id == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(1);
            id.writeTo(out);
        }
    }

    private MessageId readMessageId(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            MessageId id = new MessageId();
            id.readFrom(in);
            return id;
        }
    }
}