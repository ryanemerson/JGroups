package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.aramis.RMCastHeader;
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
    private Profiler profiler;

    // TODO remove
    private Map<Address, RMCastHeader> msgRecord = new HashMap<Address, RMCastHeader>();
    private boolean checkTotalOrder = true;

    // TODO remove
    private void checkTotalOrder(Message message) {
        if (downProtocol.getName().equals("Aramis")) {
            RMCastHeader h = (RMCastHeader) message.getHeader((short) 1008);
            RMCastHeader oldHeader = msgRecord.put(h.getId().getOriginator(), h);
            if (oldHeader != null && oldHeader.getId().getSequence() + 1 != h.getId().getSequence())
                for (long i = oldHeader.getId().getSequence() + 1; i < h.getId().getSequence(); i++)
                    log.error("ERROR!!!!!!!! Sequence missing := " + i + " | from " + h.getId().getOriginator());

            for (RMCastHeader old : msgRecord.values()) {
                if (old.getId().getTimestamp() > h.getId().getTimestamp())
                    log.error("TOTAL ORDER ERROR!!! previous message has a newer timestamp | old := " + old.getId() + " | new := " + h.getId());
            }
        }
    }

    public OrderingBox(short id, Log log, Protocol downProtocol, ViewManager viewManager, List<Address> boxMembers, Profiler profiler) {
        this.id = id;
        this.log = log;
        this.downProtocol = downProtocol;
        this.viewManager = viewManager;
        this.boxMembers = boxMembers;
        this.profiler = profiler;
        orderStore = Collections.synchronizedMap(new HashMap<Address, Long>());
        globalSequence = new AtomicInteger();
        requestCache = Collections.synchronizedSet(new HashSet<MessageId>());
    }

    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void handleOrderingRequests(Collection<MessageInfo> requests) {
        if (log.isDebugEnabled())
            log.debug("Received Multiple Ordering requests (" + requests.size() + "), send to all box members | " + requests);

        for (MessageInfo info : requests)
            requestCache.add(info.getId());

        DecoupledHeader bundledHeader = DecoupledHeader.createBundledMessage(requests);
        sendToAllBoxMembers(bundledHeader);

        profiler.requestsReceived(requests.size());
    }

    public void handleOrderingRequest(MessageInfo messageInfo) {
        if (log.isDebugEnabled())
            log.debug("Received request, send to all box members | " + messageInfo);

        requestCache.add(messageInfo.getId());
        DecoupledHeader header = DecoupledHeader.createBoxOrdering(messageInfo);

        sendToAllBoxMembers(header);

        profiler.requestReceived();
    }

    private void sendToAllBoxMembers(DecoupledHeader header) {
        // AnycastAddress is ok because TOA is the protocol below, when we are box member
        // If TOA is not used, a requirement of the protocol below is that it accepts Anycast Addresses
        AnycastAddress anycastAddress = new AnycastAddress(boxMembers);
        Message message = new Message(anycastAddress).putHeader(id, header);

        // TODO make size a variable
        int diff = 1000 - header.size();
        if (diff > 0)
            message.setBuffer(new byte[diff]); // Necessary to ensure that each msgs size is 1kb, necessary for accurate network measurements

        if (log.isTraceEnabled())
            log.trace("Send ordering to := " + anycastAddress);

        downProtocol.down(new Event(Event.MSG, message));

        profiler.totalOrderSent();
    }

    public void receiveMultipleOrderings(DecoupledHeader header, Message message) {
        Collection<MessageInfo> orderings = header.getBundledMsgInfo();
        if (log.isTraceEnabled())
            log.trace("Received multiple orderings (" + orderings.size() + ")| " + orderings);

        if (checkTotalOrder)
            checkTotalOrder(message);

        for (MessageInfo info : orderings)
            receiveOrdering(info);
    }

    public void receiveOrdering(DecoupledHeader header, Message message) {
        MessageInfo messageInfo  = header.getMessageInfo();
        if (log.isTraceEnabled())
            log.trace("Received Ordering message | " + messageInfo);

        if (checkTotalOrder)
            checkTotalOrder(message);

        receiveOrdering(messageInfo);
    }

    private void receiveOrdering(MessageInfo messageInfo) {
        // Once a message has been received at this layer, it will have been received at others (at least in the same order)
        // Increment sequence, retrieve ordering request, place into ordered list
        // If you are the source of the message: update ordering request and return to the originator
        messageInfo.setOrdering(globalSequence.incrementAndGet());

        // Prepare header lastOrderSequence and save messageOrdering
        setLastOrderSequences(messageInfo);

        if (log.isDebugEnabled())
            log.debug("Global Sequence := " + globalSequence.intValue() + " | " + messageInfo.getId());

        // If the messageId is in the requestCache then this node handled the original request, send a response
        if (requestCache.contains(messageInfo.getId()))
            sendOrderingResponse(messageInfo);
        else if (log.isTraceEnabled())
            log.trace("Don't respond request did not originate here | " + messageInfo.getOrdering() + " | " + messageInfo.getId());

        profiler.orderReceived();
    }

    private void sendOrderingResponse(MessageInfo messageInfo) {
        if (log.isTraceEnabled())
            log.trace("Send ordering response | " + messageInfo);

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