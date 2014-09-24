package org.jgroups.protocols.abaas;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Header file that is used to send all messages associated with the AbaaS protocol.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
final public class AbaaSHeader extends Header {

    public static final byte BOX_MEMBER = 1; // Announcing box Members
    public static final byte BOX_REQUEST = 2; // A request to the box
    public static final byte BOX_RESPONSE = 3; // A response from the box
    public static final byte BOX_ORDERING = 4; // Ordering a boxRequest
    public static final byte BROADCAST = 5; // Actual broadcsat of a message to anycast destinations
    public static final byte SINGLE_DESTINATION = 6; // Request for a missing message (shouldn't be necessary)
    public static final byte BUNDLED_MESSAGE = 7; // A AbaaS message that contains multiple requests

    private byte type = 0;
    private MessageInfo messageInfo = null;
    private Collection<MessageInfo> bundledMsgInfo = null;

    public static AbaaSHeader createBoxMember() {
        return new AbaaSHeader(BOX_MEMBER);
    }

    public static AbaaSHeader createBoxRequest(MessageInfo info) {
        return new AbaaSHeader(BOX_REQUEST, info);
    }

    public static AbaaSHeader createBoxResponse(MessageInfo info) {
        return new AbaaSHeader(BOX_RESPONSE, info);
    }

    public static AbaaSHeader createBoxOrdering(MessageInfo info) {
        return new AbaaSHeader(BOX_ORDERING, info);
    }

    public static AbaaSHeader createBroadcast(MessageInfo info) {
        return new AbaaSHeader(BROADCAST, info);
    }

    public static AbaaSHeader createSingleDestination(MessageInfo info) {
        return new AbaaSHeader(SINGLE_DESTINATION, info);
    }

    public static AbaaSHeader createBundledMessage(Collection<MessageInfo> requestHeaders) {
        return new AbaaSHeader(BUNDLED_MESSAGE, requestHeaders);
    }

    public AbaaSHeader() {
    }

    public AbaaSHeader(byte type) {
        this.type = type;
    }

    public AbaaSHeader(byte type, MessageInfo messageInfo) {
        this.type = type;
        this.messageInfo = messageInfo;
    }

    public AbaaSHeader(byte type, Collection<MessageInfo> bundledMsgInfo) {
        this.type = type;
        this.bundledMsgInfo = bundledMsgInfo;
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

    public Collection<MessageInfo> getBundledMsgInfo() {
        return bundledMsgInfo;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + (messageInfo != null ? messageInfo.size() : 0) + (bundledMsgInfo != null ? getBundledSize() : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        writeMessageInfo(messageInfo, out);
        writeBundledMsgInfo(bundledMsgInfo, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageInfo = readMessageInfo(in);
        bundledMsgInfo = readBundledMsgInfo(in);
    }

    @Override
    public String toString() {
        return "AbaaSHeader{" +
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
            case SINGLE_DESTINATION:    return "SINGLE_DESTINATION";
            case BUNDLED_MESSAGE:       return "BUNDLED_MESSAGE";
            default:                    return "UNDEFINED(" + t + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbaaSHeader that = (AbaaSHeader) o;

        if (type != that.type) return false;
        if (bundledMsgInfo != null ? !bundledMsgInfo.equals(that.bundledMsgInfo) : that.bundledMsgInfo != null)
            return false;
        if (messageInfo != null ? !messageInfo.equals(that.messageInfo) : that.messageInfo != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + (messageInfo != null ? messageInfo.hashCode() : 0);
        result = 31 * result + (bundledMsgInfo != null ? bundledMsgInfo.hashCode() : 0);
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

    private int getBundledSize() {
        int size = 0;
        for (MessageInfo info : bundledMsgInfo)
            size += info.size();
        return size;
    }

    private void writeBundledMsgInfo(Collection<MessageInfo> bundledHeaders, DataOutput out) throws Exception{
        if (bundledHeaders == null) {
            out.writeShort(-1);
            return;
        }

        out.writeShort(bundledHeaders.size());
        for (MessageInfo info : bundledHeaders)
            Util.writeStreamable(info, out);
    }

    private Collection<MessageInfo> readBundledMsgInfo(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;

        Collection<MessageInfo> bundledHeaders = new ArrayList<MessageInfo>();
        for (int i = 0; i < length; i++)
            bundledHeaders.add((MessageInfo) Util.readStreamable(MessageInfo.class, in));
        return bundledHeaders;
    }
}