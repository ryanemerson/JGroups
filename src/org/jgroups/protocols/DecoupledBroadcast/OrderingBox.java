package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class OrderingBox {
    private int orderListSize = 50;

    private short id;
    private Protocol downProtocol;
    private List<Address> boxMembers;
    private BlockingQueue<MessageInfo> orderQueue;
    private AtomicInteger globalSequence;
    private Set<MessageId> requestCache;

    public OrderingBox(short id, Protocol downProtocol, List<Address> boxMembers) {
        this.id = id;
        this.downProtocol = downProtocol;
        this.boxMembers = boxMembers;
        orderQueue = new ArrayBlockingQueue<MessageInfo>(orderListSize);
        globalSequence = new AtomicInteger();
        requestCache = new HashSet<MessageId>();
    }

    public void handleOrderingRequest(MessageInfo messageInfo) {
        System.out.println("Ordering request received | " + messageInfo);
        requestCache.add(messageInfo.getId());
        // Send TOA message to all boxMembers
        sendToAllBoxMembers(messageInfo);
    }

    private void sendToAllBoxMembers(MessageInfo messageInfo) {
        System.out.println("Send request to all box members | " + messageInfo);
        // Forward request to all box members
        DecoupledHeader header = DecoupledHeader.createBoxOrdering(messageInfo);

        // AnycastAddress is ok because TOA is the protocol below, when we are box member
        // If TOA is not used, a requirement of the protocol below is that it accepts Anycast Addresses
        Message message = new Message(new AnycastAddress(boxMembers));
        message.putHeader(id, header);
        downProtocol.down(new Event(Event.MSG, message));
    }

    public void receiveOrdering(MessageInfo messageInfo) {
        System.out.println("Receive Ordering message | " + messageInfo);
        // Once a message has been received at this layer, it will have been received at others (at least in the same order)
        // Increment sequence, retrive ordering request, place into ordered list
        // If you are the source of the message: update ordering request and return to the originator
        messageInfo.setOrdering(globalSequence.incrementAndGet());
        addOrderingToQueue(messageInfo);

        System.out.println("Global Sequence := " + globalSequence.intValue());

        // If the messageId is in the requestCache then this node handled the original request, send a response
        if (requestCache.contains(messageInfo.getId()))
            sendOrderingResponse(messageInfo);
    }

    private void sendOrderingResponse(MessageInfo messageInfo) {
        System.out.println("Send ordering response | " + messageInfo);
        // Send the latest version of the order list to the src of the orderRequest
        List<MessageInfo> orderList = new ArrayList<MessageInfo>(orderQueue);
        DecoupledHeader header = DecoupledHeader.createBoxResponse(messageInfo, orderList);
        Message message = new Message(messageInfo.getId().getOriginator());
        message.putHeader(id, header);
        downProtocol.down(new Event(Event.MSG, message));
        requestCache.remove(messageInfo.getId()); // Remove old id
    }

    private void addOrderingToQueue(MessageInfo message) {
        if (orderQueue.remainingCapacity() == 0)
            orderQueue.poll();

        orderQueue.add(message);
    }
}
