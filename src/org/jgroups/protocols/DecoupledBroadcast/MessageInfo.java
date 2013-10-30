package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class MessageInfo implements Comparable<MessageInfo>, SizeStreamable {
    private MessageId id = null;
    private long ordering = -1; // Sequence provided by the BOX, value created after TOA and before placed in the queue
    private Collection<Address> destinations = null; // Addresses of the nodes that the message is intended for

    public MessageInfo() {
    }

    public MessageInfo(MessageId id, Collection<Address> destinations) {
        this (id, -1, destinations);
    }

    public MessageInfo(MessageId id, long ordering, Collection<Address> destinations) {
        this.id = id;
        this.ordering = ordering;
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

    public Collection<Address> getDestinations() {
        return destinations;
    }

    public void setDestinations(Collection<Address> destinations) {
        this.destinations = destinations;
    }

    @Override
    public int size() {
        return id.size() + Global.INT_SIZE + (int) Util.size(destinations);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        writeMessageId(id, out);
        Util.writeLong(ordering, out);
        Util.writeAddresses(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = readMessageId(in);
        ordering = Util.readLong(in);
        destinations = (Collection<Address>) Util.readAddresses(in, ArrayList.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageInfo that = (MessageInfo) o;

        if (ordering != that.ordering) return false;
        if (destinations != null ? !destinations.equals(that.destinations) : that.destinations != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (ordering ^ (ordering >>> 32));
        result = 31 * result + (destinations != null ? destinations.hashCode() : 0);
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
                ", destinations=" + destinations +
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