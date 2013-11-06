package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.tom.ToaHeader;
import org.jgroups.stack.Protocol;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class Decoupled extends Protocol {

    @Property(name = "box_member", description = "Is this node a box member")
    private boolean boxMember = false;

    private Address localAddress = null;
    private final ViewManager viewManager = new ViewManager();
    private final DeliveryManager deliveryManager = new DeliveryManager(log, viewManager);
    // Store messages before ordering request is sent
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private final List<Address> boxMembers = new ArrayList<Address>();
    private View view = null;
    private OrderingBox box;
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private Random random = new Random(); // Random object for selecting which box member to use
    private ExecutorService executor;
    // If true then ordering requests will only be sent to one box member, otherwise all box members are used
    private final boolean singleOrderPoint = true;

    public Decoupled() {
    }

    @Override
    public void init() throws Exception {
        setLevel("info");
        if (boxMember)
            box = new OrderingBox(id, log, down_prot, viewManager, boxMembers);
    }

    @Override
    public void start() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());

        if (boxMember)
            getTransport().getTimer().schedule(new BoxMemberAnnouncement(), 20, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        executor.shutdown();
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                DecoupledHeader header = (DecoupledHeader) message.getHeader(id);
                if (header == null)
                    break;

                switch (header.getType()) {
                    case DecoupledHeader.BOX_MEMBER:
                        boxMembers.add(message.getSrc());
                        if (log.isInfoEnabled())
                            log.info("Box Member discovered | " + message.getSrc());
                        break;
                    case DecoupledHeader.BOX_REQUEST:
                        box.handleOrderingRequest(header.getMessageInfo());
                        break;
                    case DecoupledHeader.BOX_ORDERING:
                        ToaHeader h = (ToaHeader) message.getHeader((short) 58);
                        log.debug("TOA Header In SWITCH := " + h.getSequencerNumber() + " | ID := " + h.getMessageID());
                        box.receiveOrdering(header.getMessageInfo(), message);
                        break;
                    case DecoupledHeader.BOX_RESPONSE:
                        handleOrderingResponse(header);
                        break;
                    case DecoupledHeader.BROADCAST:
                        handleBroadcast(header, message);
                        break;
                    case DecoupledHeader.SINGLE_DESTINATION:
                        handleSingleDestination(message);
                        break;
                }
                return null;
            case Event.VIEW_CHANGE:
                view = (View) event.getArg();
                viewManager.setCurrentView(view);
                log.debug("New View := " + view);
                break;
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                handleMessageRequest(event);
                return null;
            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                deliveryManager.setLocalAddress(localAddress);
                if (boxMember)
                    box.setLocalAddress(localAddress);
                break;
        }
        return down_prot.down(event);
    }

    private boolean checkIfDestinationIsBox(AnycastAddress anycastAddress) {
        for (Address address : anycastAddress.getAddresses()) {
            if (boxMembers.contains(address))
                return true;
        }
        return false;
    }

    private void handleMessageRequest(Event event) {
        if (log.isTraceEnabled())
            log.trace("Handle Message Request");

        Message message = (Message) event.getArg();
        Address destination = message.getDest();

        if (destination != null && destination instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
            if (checkIfDestinationIsBox((AnycastAddress) destination)) {
                throw new IllegalArgumentException("Can't send message to box member!");
            } else {
                sendTotalOrderAnycast(((AnycastAddress) destination).getAddresses(), message);
            }
        } else if (destination != null && destination instanceof AnycastAddress) {
            sendNoTotalOrderAnycast(((AnycastAddress)destination).getAddresses(),message);
        } else {
            down_prot.down(event);
        }
    }

    private void sendNoTotalOrderAnycast(Collection<Address> destinations, Message message) {
        if (destinations == null) {
            down_prot.down(new Event(Event.MSG, message));
        } else {
            message.src(localAddress);
            for (Address destination : destinations) {
                Message messageCopy = message.copy();
                messageCopy.setDest(destination);
                down_prot.down(new Event(Event.MSG, messageCopy));
            }
        }
    }

    private void sendTotalOrderAnycast(Collection<Address> destinations, Message message) {
        if (destinations.isEmpty())
            destinations.addAll(view.getMembers());

        for (Address box : boxMembers) {
            if (destinations.contains(box))
                throw new IllegalArgumentException("A Box member is not a valid destination!");
        }

        if (destinations.size() == 1) {
            MessageId messageId = new MessageId(localAddress, localSequence.getAndIncrement()); // Increment localSequence
            MessageInfo messageInfo = new MessageInfo(messageId, view.getViewId(), viewManager.getDestinationsAsByteArray(destinations));
            message.putHeader(id, DecoupledHeader.createSingleDestination(messageInfo));
            message.setDest(destinations.iterator().next());
            down_prot.down(new Event(Event.MSG, message));
        } else {
            sendOrderingRequest(destinations, message);
        }
    }

    private void sendOrderingRequest(Collection<Address> destinations, Message message) {
        if (destinations.isEmpty())
            destinations.addAll(view.getMembers());

        // Create messageId for this message and store it for later
        MessageId messageId = new MessageId(localAddress, localSequence.getAndIncrement()); // Increment localSequence
//        deliveryManager.addMessageToStore(messageId, message);
        messageStore.put(messageId, message);

        byte[] dest = viewManager.getDestinationsAsByteArray(destinations);
//        log.debug("Send ordering request | " + messageId + " | dest " + destinations + " | view " + view + " | byte[]" + Arrays.toString(dest));


        MessageInfo messageInfo = new MessageInfo(messageId, view.getViewId(), dest);
        DecoupledHeader header = DecoupledHeader.createBoxRequest(messageInfo);

        Address destination = singleOrderPoint ? boxMembers.get(0) : boxMembers.get(random.nextInt(boxMembers.size())); // Select box at random;
        Message requestMessage = new Message(destination).src(localAddress).putHeader(id, header);
        down_prot.down(new Event(Event.MSG, requestMessage));

        if (log.isTraceEnabled())
            log.trace("Ordering Request Sent to " + destination + " | " + header);
    }

    private void handleOrderingResponse(DecoupledHeader responseHeader) {
        if (log.isTraceEnabled())
            log.trace("Ordering response received | " + responseHeader);
        // Receive the ordering list and send this message to all nodes in the destination set

//        log.debug("Ordering Response received | " + responseHeader.getMessageInfo().getOrdering());
//        log.debug("Ordering received| " + responseHeader.getMessageInfo().getId() + " | dest " + Arrays.toString(responseHeader.getMessageInfo().getDestinations()) +  " | " + viewManager.getDestinations(responseHeader.getMessageInfo()));

        MessageInfo messageInfo = responseHeader.getMessageInfo();
        DecoupledHeader header = DecoupledHeader.createBroadcast(messageInfo);
        Message message = messageStore.get(messageInfo.getId());
        message.putHeader(id, header);

        broadcastMessage(viewManager.getDestinations(messageInfo), message);
    }

    private void broadcastMessage(Collection<Address> destinations, Message message) {
        if (log.isTraceEnabled())
            log.trace("Broadcast Message to | " + destinations);

        boolean deliverToSelf = destinations.contains(localAddress);
        // Send the message to all destinations
//        log.debug("Broadcast Message " + ((DecoupledHeader)message.getHeader(id)).getMessageInfo().getOrdering() +
//                " to | " + destinations + " | deliverToSelf " + deliverToSelf);
        for (Address destination : destinations) {
            if (destination.equals(localAddress))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
//            log.debug("Broadcast Sent := " + ((DecoupledHeader)message.getHeader(id)).getMessageInfo().getOrdering() + " | To := " + destination);
        }

        DecoupledHeader header = (DecoupledHeader)message.getHeader(id);
        if (deliverToSelf) {
            message.setDest(localAddress);
            deliveryManager.addMessageToDeliver(header, message);
        } else {
            messageStore.remove(header.getMessageInfo().getId());
        }
    }

    private void handleBroadcast(DecoupledHeader header, Message message) {
        if (log.isTraceEnabled())
            log.trace("Broadcast received | " + header);
//        log.debug("Broadcast received | " + header.getMessageInfo().getOrdering());
        deliveryManager.addMessageToDeliver(header, message);
    }

    private void handleSingleDestination(Message message) {
        if (log.isTraceEnabled())
            log.trace("Single Destination Message received | " + message.getHeader(id));
        deliveryManager.addSingleDestinationMessage(message);
    }

    private void deliverMessage(Message message) {
        MessageId id = ((DecoupledHeader)message.getHeader(this.id)).getMessageInfo().getId();
        messageStore.remove(id);
        message.setDest(localAddress);

        if (log.isTraceEnabled())
            log.trace("Deliver Message | " + message);

//        DecoupledHeader header = (DecoupledHeader)message.getHeader(id);
//        log.debug("*****Deliver Message | " + viewManager.getDestinations(header.getMessageInfo()) + " : " + header.getMessageInfo().getOrdering());
        up_prot.up(new Event(Event.MSG, message));
    }

    final class DeliverMessages implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    List<Message> messages = deliveryManager.getNextMessagesToDeliver();
                    for(Message message : messages) {
                        try {
                            deliverMessage(message);
                        } catch(Throwable t) {
                            log.warn("Exception caught while delivering message " + message + ":" + t.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    final class BoxMemberAnnouncement implements Runnable {
        @Override
        public void run() {
            final Message message = new Message(null, localAddress, new byte[0]).putHeader(id, DecoupledHeader.createBoxMember());
            Event event = new Event(Event.MSG, message);
            down_prot.down(event);

            if (log.isInfoEnabled())
                log.info("I am Box Message Sent | " + message);
        }
    }
}