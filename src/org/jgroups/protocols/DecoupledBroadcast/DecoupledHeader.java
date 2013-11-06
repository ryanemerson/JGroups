package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
final public class DecoupledHeader extends Header {

    public static final byte BOX_MEMBER = 1; // Announcing box Members
    public static final byte BOX_REQUEST = 2; // A request to the box
    public static final byte BOX_RESPONSE = 3; // A response from the box
    public static final byte BOX_ORDERING = 4; // Ordering a boxRequest
    public static final byte BROADCAST = 5; // Actual broadcsat of a message to anycast destinations
    public static final byte SINGLE_DESTINATION = 6; // Request for a missing message (shouldn't be necessary)

    private byte type = 0;
    private MessageInfo messageInfo = null;

    public DecoupledHeader() {
    }

    public static DecoupledHeader createBoxMember() {
        return new DecoupledHeader(BOX_MEMBER, null);
    }

    public static DecoupledHeader createBoxRequest(MessageInfo info) {
        return new DecoupledHeader(BOX_REQUEST, info);
    }

    public static DecoupledHeader createBoxResponse(MessageInfo info) {
        return new DecoupledHeader(BOX_RESPONSE, info);
    }

    public static DecoupledHeader createBoxOrdering(MessageInfo info) {
        return new DecoupledHeader(BOX_ORDERING, info);
    }

    public static DecoupledHeader createBroadcast(MessageInfo info) {
        return new DecoupledHeader(BROADCAST, info);
    }

    public static DecoupledHeader createSingleDestination(MessageInfo info) {
        return new DecoupledHeader(SINGLE_DESTINATION, info);
    }

    public DecoupledHeader(byte type, MessageInfo messageInfo) {
        this.type = type;
        this.messageInfo = messageInfo;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public MessageInfo getMessageInfo() {
        return messageInfo;
    }

    public void setMessageInfo(MessageInfo messageInfo) {
        this.messageInfo = messageInfo;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + (messageInfo != null ? messageInfo.size() : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        writeMessageInfo(messageInfo, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageInfo = readMessageInfo(in);
    }

    @Override
    public String toString() {
        return "DecoupledHeader{" +
                "type=" + type2String(type) +
                ", messageInfo=" + messageInfo +
                '}';
    }

    public static String type2String(int t) {
        switch(t) {
            case BOX_MEMBER:            return "BOX_MEMBER";
            case BOX_REQUEST:           return "BOX_REQUEST";
            case BOX_RESPONSE:          return "BOX_RESPONSE";
            case BOX_ORDERING:          return "BOX_ORDERING";
            case BROADCAST:	            return "BROADCAST";
            case SINGLE_DESTINATION:       return "SINGLE_DESTINATION";
            default:                        return "UNDEFINED(" + t + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecoupledHeader that = (DecoupledHeader) o;

        if (type != that.type) return false;
        if (messageInfo != null ? !messageInfo.equals(that.messageInfo) : that.messageInfo != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + (messageInfo != null ? messageInfo.hashCode() : 0);
        return result;
    }

    private void writeMessageInfo(MessageInfo info, DataOutput out) throws Exception {
        if (info == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(1);
            info.writeTo(out);
        }
    }

    private MessageInfo readMessageInfo(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) {
            return null;
        } else {
            MessageInfo info = new MessageInfo();
            info.readFrom(in);
            return info;
        }
    }
}