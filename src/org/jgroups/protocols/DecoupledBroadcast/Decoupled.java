package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
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
    private final DeliveryManager deliveryManager = new DeliveryManager(viewManager, localAddress);
    private View view = null;
    private List<Address> boxMembers = new ArrayList<Address>();
    private OrderingBox box;
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private Random random = new Random(); // Random object for selecting which box member to use
    private ExecutorService executor;

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
                        box.receiveOrdering(header.getMessageInfo());
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
        if (log.isDebugEnabled())
            log.debug("Handle Message Request");

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
        deliveryManager.addMessageToStore(messageId, message);

        MessageInfo messageInfo = new MessageInfo(messageId, view.getViewId(), viewManager.getDestinationsAsByteArray(destinations));
        DecoupledHeader header = DecoupledHeader.createBoxRequest(messageInfo);
        Address destination = boxMembers.get(random.nextInt(boxMembers.size())); // Select box at random
        Message requestMessage = new Message(destination).src(localAddress).putHeader(id, header);
        down_prot.down(new Event(Event.MSG, requestMessage));

        if (log.isDebugEnabled())
            log.debug("Ordering Request Sent to " + destination + " | " + header);
    }

    private void handleOrderingResponse(DecoupledHeader responseHeader) {
        if (log.isDebugEnabled())
            log.debug("Ordering response received | " + responseHeader);
        // Receive the ordering list and send this message to all nodes in the destination set

        MessageInfo messageInfo = responseHeader.getMessageInfo();
        DecoupledHeader header = DecoupledHeader.createBroadcast(messageInfo, responseHeader.getOrderList());
        Message message = deliveryManager.getMessageFromStore(messageInfo.getId());
        message.putHeader(id, header);

        broadcastMessage(viewManager.getDestinations(messageInfo), message);
    }

    private void broadcastMessage(Collection<Address> destinations, Message message) {
        if (log.isDebugEnabled())
            log.debug("Broadcast Message to | " + destinations);

        boolean deliverToSelf = destinations.contains(localAddress);
        // Send the message to all destinations
        for (Address destination : destinations) {
            if (destination.equals(localAddress))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
        }

        if (deliverToSelf)
            deliveryManager.addMessageToDeliver((DecoupledHeader)message.getHeader(id), message);
    }

    private void handleBroadcast(DecoupledHeader header, Message message) {
        if (log.isDebugEnabled())
            log.debug("Broadcast received | " + header);
        deliveryManager.addMessageToDeliver(header, message);
    }

    private void handleSingleDestination(Message message) {
        if (log.isDebugEnabled())
            log.debug("Single Destination Message received | " + message.getHeader(id));
        deliveryManager.addSingleDestinationMessage(message);
    }

    private void deliverMessage(Message message) {
        message.setDest(localAddress);

        if (log.isDebugEnabled())
            log.debug("Deliver Message | " + message);

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