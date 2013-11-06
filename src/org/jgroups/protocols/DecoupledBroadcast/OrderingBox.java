package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.stack.Protocol;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class OrderingBox {
    private final short id;
    private final Log log;
    private final Protocol downProtocol;
    private final ViewManager viewManager;
    private final List<Address> boxMembers;
    private final Map<Address, Long> orderStore;
    private final AtomicInteger globalSequence;
    private final Set<MessageId> requestCache;
    private Address localAddress;

    public OrderingBox(short id, Log log, Protocol downProtocol, ViewManager viewManager, List<Address> boxMembers) {
        this.id = id;
        this.log = log;
        this.downProtocol = downProtocol;
        this.viewManager = viewManager;
        this.boxMembers = boxMembers;
        orderStore = Collections.synchronizedMap(new HashMap<Address, Long>());
        globalSequence = new AtomicInteger();
        requestCache = Collections.synchronizedSet(new HashSet<MessageId>());
    }

    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void handleOrderingRequest(MessageInfo messageInfo) {
        if (log.isTraceEnabled())
            log.trace("Ordering request received | " + messageInfo);
        requestCache.add(messageInfo.getId());
        log.debug("Cache contains " + messageInfo.getId() + " := " + requestCache.contains(messageInfo.getId()));
        log.debug("Received ordering request | " + messageInfo.getOrdering() + " | " + messageInfo.getId());
        // Send TOA message to all boxMembers
        sendToAllBoxMembers(messageInfo);
    }

    private void sendToAllBoxMembers(MessageInfo messageInfo) {
        if (log.isTraceEnabled())
            log.trace("Send request to all box members | " + messageInfo);
        // Forward request to all box members
        DecoupledHeader header = DecoupledHeader.createBoxOrdering(messageInfo);

        // AnycastAddress is ok because TOA is the protocol below, when we are box member
        // If TOA is not used, a requirement of the protocol below is that it accepts Anycast Addresses
//        Message message = new Message(new AnycastAddress(boxMembers)).src(localAddress).putHeader(id, header);
        AnycastAddress anycastAddress = new AnycastAddress(boxMembers);
        log.debug("Send ordering to := " + anycastAddress);
        Message message = new Message(anycastAddress).putHeader(id, header);
        downProtocol.down(new Event(Event.MSG, message));
    }

    public void receiveOrdering(MessageInfo messageInfo, Message message) {
        if (log.isTraceEnabled())
            log.trace("Receive Ordering message | " + messageInfo);

        // Once a message has been received at this layer, it will have been received at others (at least in the same order)
        // Increment sequence, retrive ordering request, place into ordered list
        // If you are the source of the message: update ordering request and return to the originator
        messageInfo.setOrdering(globalSequence.incrementAndGet());

        log.debug(messageInfo.getId() + " | ordering := " + messageInfo.getOrdering());
        // Prepare header lastOrderSequence and save messageOrdering
        setLastOrderSequences(messageInfo);

        if (log.isTraceEnabled())
            log.trace("Global Sequence := " + globalSequence.intValue());

        // If the messageId is in the requestCache then this node handled the original request, send a response
        if (requestCache.contains(messageInfo.getId()))
            sendOrderingResponse(messageInfo);
        else
            log.debug("Don't respond request did not originate here | " + messageInfo.getOrdering() + " | " + messageInfo.getId());
    }

    private void sendOrderingResponse(MessageInfo messageInfo) {
        if (log.isTraceEnabled())
            log.trace("Send ordering response | " + messageInfo);

        log.debug("Send ordering response | " + messageInfo.getOrdering() + " | " + messageInfo.getId());

        // Send the latest version of the order list to the src of the orderRequest
        DecoupledHeader header = DecoupledHeader.createBoxResponse(messageInfo);
        Message message = new Message(messageInfo.getId().getOriginator()).src(localAddress).putHeader(id, header);
        downProtocol.down(new Event(Event.MSG, message));
        requestCache.remove(messageInfo.getId()); // Remove old id
    }

    private void setLastOrderSequences(MessageInfo messageInfo) {
        long[] lastOrderSequences = new long[messageInfo.getDestinations().length];
        List<Address> destinations = viewManager.getDestinations(messageInfo);
        for (int i = 0; i < destinations.size(); i++) {
            Address destination = destinations.get(i);
            if (!orderStore.containsKey(destination)) {
                lastOrderSequences[i] = -1;
                orderStore.put(destinations.get(i), messageInfo.getOrdering());
            } else {
                lastOrderSequences[i] = orderStore.put(destination, messageInfo.getOrdering());
            }
        }
        messageInfo.setLastOrderSequence(lastOrderSequences);
    }
}