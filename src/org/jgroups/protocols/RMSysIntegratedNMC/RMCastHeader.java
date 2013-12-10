package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
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
public class RMCastHeader extends Header {
    public static final byte EMPTY_PROBE_MESSAGE = 1;
    public static final byte BROADCAST_MESSAGE = 2;

    private byte type = 0;
    private MessageId id = null; // Id object for this message
    private Address disseminator = null; // Address of the node currently disseminating this message
    private int copy = 0; // The copy of this node
    private NMCData nmcData = null;
    private Collection<Address> destinations = new ArrayList<Address>();

    public RMCastHeader() {
    }

    public static RMCastHeader createEmptyProbeHeader(MessageId id, Address disseminator, NMCData data, Collection<Address> destinations) {
        return new RMCastHeader(EMPTY_PROBE_MESSAGE, id, disseminator, 0, data, destinations);
    }

    public static RMCastHeader createBroadcastHeader(MessageId id, Address disseminator, int copy, NMCData nmcData, Collection<Address> destinations) {
        return new RMCastHeader(BROADCAST_MESSAGE, id, disseminator, copy, nmcData, destinations);
    }

    private RMCastHeader(byte type, MessageId id, Address disseminator, int copy, NMCData nmcData, Collection<Address> destinations) {
        this.type = type;
        this.id = id;
        this.disseminator = disseminator;
        this.copy = copy;
        this.nmcData = nmcData;
        this.destinations = destinations;
    }

    public MessageId getId() {
        return id;
    }

    public byte getType() {
        return type;
    }

    public Address getDisseminator() {
        return disseminator;
    }

    public void setDisseminator(Address disseminator) {
        this.disseminator = disseminator;
    }

    public int getCopy() {
        return copy;
    }

    public void setCopy(int copy) {
        this.copy = copy;
    }

    public int getCopyTotal() {
        // Returning -1 for a probe message ensures that only copy 0 of a probe message is sent
        return type == EMPTY_PROBE_MESSAGE ? -1 : nmcData.getMessageCopies();
    }

    public NMCData getNmcData() {
        return nmcData;
    }

    public Collection<Address> getDestinations() {
        return destinations;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + id.size() + Util.size(disseminator) + Global.INT_SIZE +
                (nmcData == null ? 0 : nmcData.size() + (int) Util.size(destinations));
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        Util.writeStreamable(id, out);
        Util.writeAddress(disseminator, out);
        out.writeInt(copy);
        Util.writeStreamable(nmcData, out);
        Util.writeAddresses(destinations, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        id = (MessageId) Util.readStreamable(MessageId.class, in);
        disseminator = Util.readAddress(in);
        copy = in.readInt();
        nmcData = (NMCData) Util.readStreamable(NMCData.class, in);
        destinations= (Collection<Address>) Util.readAddresses(in, ArrayList.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RMCastHeader that = (RMCastHeader) o;

        if (copy != that.copy) return false;
        if (type != that.type) return false;
        if (destinations != null ? !destinations.equals(that.destinations) : that.destinations != null) return false;
        if (disseminator != null ? !disseminator.equals(that.disseminator) : that.disseminator != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (nmcData != null ? !nmcData.equals(that.nmcData) : that.nmcData != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (disseminator != null ? disseminator.hashCode() : 0);
        result = 31 * result + copy;
        result = 31 * result + (nmcData != null ? nmcData.hashCode() : 0);
        result = 31 * result + (destinations != null ? destinations.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RMCastHeader{" +
                "type=" + type2String(type) +
                ", id=" + id +
                ", disseminator=" + disseminator +
                ", copy=" + copy +
                ", nmcData=" + nmcData +
                ", destinations=" + destinations +
                '}';
    }

    public static String type2String(int t) {
        switch (t) {
            case EMPTY_PROBE_MESSAGE:
                return "EMPTY_PROBE_MESSAGE";
            case BROADCAST_MESSAGE:
                return "BROADCAST_MESSAGE";
            default:
                return "UNDEFINED(" + t + ")";
        }
    }
}