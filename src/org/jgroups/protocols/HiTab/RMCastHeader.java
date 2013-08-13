package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
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
public class RMCastHeader extends Header {
    private MessageId id = null; // Id object for this message
    private Address disseminator = null; // Address of the node currently disseminating this message
    private int copy = 0; // The copy of this node
    private int copyTotal = 0; // Total number of copies to be broadcast

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

    public Address getOriginator() {
        return id.getOriginator();
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

    @Override
    public int size() {
        return id.size() + Util.size(disseminator) + Util.size(copy) + Util.size(copyTotal);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
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
    }

    @Override
    public String toString() {
        return "RMCastHeader{" +
                "id=" + id +
                ", disseminator=" + disseminator +
                ", copy=" + copy +
                ", copyTotal=" + copyTotal +
                '}';
    }
}