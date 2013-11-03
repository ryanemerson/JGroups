package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Global;
import org.jgroups.Header;

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
final public class DecoupledHeader extends Header {

    public static final byte BOX_MEMBER = 1; // Announcing box Members
    public static final byte BOX_REQUEST = 2; // A request to the box
    public static final byte BOX_RESPONSE = 3; // A response from the box
    public static final byte BOX_ORDERING = 4; // Ordering a boxRequest
    public static final byte BROADCAST = 5; // Actual broadcsat of a message to anycast destinations
    public static final byte SINGLE_DESTINATION = 6; // Request for a missing message (shouldn't be necessary)

    private byte type = 0;
    private MessageInfo messageInfo = null;
    private List<MessageInfo> orderList = null;

    public DecoupledHeader() {
    }

    public static DecoupledHeader createBoxMember() {
        return new DecoupledHeader(BOX_MEMBER, null, null);
    }

    public static DecoupledHeader createBoxRequest(MessageInfo info) {
        return new DecoupledHeader(BOX_REQUEST, info, null);
    }

    public static DecoupledHeader createBoxResponse(MessageInfo info, List<MessageInfo> orderList) {
        return new DecoupledHeader(BOX_RESPONSE, info, orderList);
    }

    public static DecoupledHeader createBoxOrdering(MessageInfo info) {
        return new DecoupledHeader(BOX_ORDERING, info, null);
    }

    public static DecoupledHeader createBroadcast(MessageInfo info, List<MessageInfo> orderList) {
        return new DecoupledHeader(BROADCAST, info, orderList);
    }

    public static DecoupledHeader createSingleDestination(MessageInfo info) {
        return new DecoupledHeader(SINGLE_DESTINATION, info, null);
    }

    public DecoupledHeader(byte type, MessageInfo messageInfo, List<MessageInfo> orderList) {
        this.type = type;
        this.messageInfo = messageInfo;
        this.orderList = orderList;
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

    public List<MessageInfo> getOrderList() {
        return orderList;
    }

    public void setOrderList(List<MessageInfo> orderList) {
        this.orderList = orderList;
    }

    @Override
    public int size() {
        return Global.BYTE_SIZE + (messageInfo != null ? messageInfo.size() : 0) + orderListSize(orderList);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeByte(type);
        writeMessageInfo(messageInfo, out);
        writeOrderList(orderList, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        type = in.readByte();
        messageInfo = readMessageInfo(in);
        orderList = readOrderList(in);
    }

    @Override
    public String toString() {
        return "DecoupledHeader{" +
                "type=" + type2String(type) +
                ", messageInfo=" + messageInfo +
                ", orderList=" + orderList +
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
        if (orderList != null ? !orderList.equals(that.orderList) : that.orderList != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + (messageInfo != null ? messageInfo.hashCode() : 0);
        result = 31 * result + (orderList != null ? orderList.hashCode() : 0);
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

    private int orderListSize(Collection<MessageInfo> orderList) {
        int size = 0;
        if(orderList != null && !orderList.isEmpty()) {
            for(MessageInfo info : orderList) {
                size += info.size();
            }
        }
        return size;
    }

    private void writeOrderList(List<MessageInfo> orderList, DataOutput out) throws Exception {
        if(orderList == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(orderList.size());
        for(MessageInfo info : orderList) {
            info.writeTo(out);
        }
    }

    private List<MessageInfo> readOrderList(DataInput in) throws Exception {
        short length = in.readShort();
        if (length < 0) return null;

        List<MessageInfo> orderList = new ArrayList<MessageInfo>();
        MessageInfo info;
        for (int i = 0; i < length; i++) {
            info = new MessageInfo();
            info.readFrom(in);
            orderList.add(info);
        }
        return orderList;
    }
}