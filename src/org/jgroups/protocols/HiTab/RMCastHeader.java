package org.jgroups.protocols.HiTab;

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
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class RMCastHeader extends Header {
    private MessageId id = null; // Id object for this message
    private Address disseminator = null; // Address of the node currently disseminating this message
    private int copy = 0; // The copy of this node
    private int copyTotal = 0; // Total number of copies to be broadcast
    private List<RMCastHeader> piggyBackedHeaders = null; // Piggybacked headers to reduce the total number of UDP messages required

    public RMCastHeader() {
    }

    public RMCastHeader(MessageId id, Address disseminator, int copy, int copyTotal) {
        this.id = id;
        this.disseminator = disseminator;
        this.copy = copy;
        this.copyTotal = copyTotal;
    }

    public RMCastHeader(Address originator, int copyTotal) {
        this(new MessageId(System.currentTimeMillis(), originator), originator, 0, copyTotal);
    }

    public MessageId getId() {
        return id;
    }

    public void setId(MessageId id) {
        this.id = id;
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
        return copyTotal;
    }

    public void setCopyTotal(int copyTotal) {
        this.copyTotal = copyTotal;
    }

    public List<RMCastHeader> getPiggyBackedHeaders() {
        return piggyBackedHeaders;
    }

    public void setPiggyBackedHeaders(List<RMCastHeader> piggyBackedHeaders) {
        this.piggyBackedHeaders = piggyBackedHeaders;
    }

    @Override
    public int size() {
        return id.size() + Util.size(disseminator) + (Global.INT_SIZE * 2) + piggyListSize(piggyBackedHeaders);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        Util.writeStreamable(id, out);
        Util.writeAddress(disseminator, out);
        out.writeInt(copy);
        out.writeInt(copyTotal);
        writePiggyList(piggyBackedHeaders, out);
    }

    public void writePiggyHeader(DataOutput out) throws Exception {
        Util.writeStreamable(id, out);
        Util.writeAddress(disseminator, out);
        out.writeInt(copy);
        out.writeInt(copyTotal);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = (MessageId) Util.readStreamable(MessageId.class, in);
        disseminator = Util.readAddress(in);
        copy = in.readInt();
        copyTotal = in.readInt();
        piggyBackedHeaders = readPiggyList(in);
    }

    public void readPiggyHeader(DataInput in) throws Exception {
        id = (MessageId) Util.readStreamable(MessageId.class, in);
        disseminator = Util.readAddress(in);
        copy = in.readInt();
        copyTotal = in.readInt();
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RMCastHeader that = (RMCastHeader) o;

        if (copy != that.copy) return false;
        if (copyTotal != that.copyTotal) return false;
        if (!id.equals(that.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + copy;
        result = 31 * result + copyTotal;
        return result;
    }

    @Override
    public String toString() {
        return "RMCastHeader{" +
                "id=" + id +
                ", disseminator=" + disseminator +
                ", copy=" + copy +
                ", copyTotal=" + copyTotal +
                ", piggyBackHeaders=" + piggyBackedHeaders +
                '}';
    }

    private int piggyListSize(Collection<RMCastHeader> piggyList) {
        int size = 0;
        if(piggyList != null && !piggyList.isEmpty()) {
            for(RMCastHeader header : piggyList) {
                size += header.size();
            }
        }
        return size;
    }

    private void writePiggyList(List<RMCastHeader> piggyList, DataOutput out) throws Exception {
        if(piggyList == null || piggyList.size() == 0) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(piggyList.size());
        for(RMCastHeader header : piggyList) {
            header.writePiggyHeader(out);
        }
    }

    private List<RMCastHeader> readPiggyList(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;
        List<RMCastHeader> piggyList = new ArrayList<RMCastHeader>();
        for (int i = 0; i < length; i++) {
            RMCastHeader header = new RMCastHeader();
            header.readPiggyHeader(in);
            piggyList.add(header);
        }
        return piggyList;
    }
}