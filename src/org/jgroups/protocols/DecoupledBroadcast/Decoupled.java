package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.MethodCall;
import org.jgroups.protocols.RMSysIntegratedNMC.PCSynch;
import org.jgroups.protocols.RMSysIntegratedNMC.RMSys;
import org.jgroups.protocols.tom.TOA;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.nio.ByteBuffer;
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

    @Property(name = "box_members", description = "A list of hostnames that will be box members (seperated by a colon)")
    private String boxHostnames = "";

    @Property(name = "total_order", description = "The name of the total order protocol to be used by box members")
    private String totalOrderProtocol = "TOA";

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
    private final boolean singleOrderPoint = false;

    public Decoupled() {
    }

    @Override
    public void init() throws Exception {
        setLevel("info");
        if (boxMember) {
            createProtocolStack();
            getTransport().getTimer().schedule(new BoxMemberAnnouncement(), 20, TimeUnit.SECONDS);

            // Must be after the protocols have been added to the stack to ensure that down_prot is set to the correct protocol
            box = new OrderingBox(id, log, down_prot, viewManager, boxMembers);
        }
    }

    private void createProtocolStack() throws Exception {
        String protocol = totalOrderProtocol;
        if (protocol.equalsIgnoreCase("TOA")) {
            TOA toa = new TOA();
            getProtocolStack().insertProtocol(toa, ProtocolStack.BELOW, this.getName());
            if (log.isInfoEnabled())
                log.info("Total Order protocol := TOA");
        } else if(protocol.equalsIgnoreCase("Hybrid")) {
            List<String> hostnames = new ArrayList<String>();
            hostnames.addAll(Arrays.asList(boxHostnames.split(":")));

            if (hostnames.isEmpty()) {
                if (log.isFatalEnabled())
                    log.fatal("Box members must be specified in the configuration file!");
                System.exit(1);
            }

            PCSynch clock = new PCSynch(hostnames);
            RMSys rmSys = new RMSys(clock, hostnames);

            // TODO change so that Events are passed up and down the stack (temporary hack)
            getProtocolStack().insertProtocol(clock, ProtocolStack.BELOW, this.getName());
            getProtocolStack().insertProtocol(rmSys, ProtocolStack.BELOW, this.getName());

            if (log.isInfoEnabled())
                log.info("Total Order protocol := Hybrid");
        } else {
            if (log.isFatalEnabled())
                log.fatal("Invalid total order protocol selected");
            System.exit(1);
        }
    }

    @Override
    public void start() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());
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
                if (log.isTraceEnabled())
                    log.trace("New View := " + view);
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
        messageStore.put(messageId, message);

        byte[] dest = viewManager.getDestinationsAsByteArray(destinations);
        if (log.isTraceEnabled())
            log.trace("Send ordering request | " + messageId + " | dest " + destinations + " | view " + view + " | byte[]" + Arrays.toString(dest));

        MessageInfo messageInfo = new MessageInfo(messageId, view.getViewId(), dest);
        DecoupledHeader header = DecoupledHeader.createBoxRequest(messageInfo);

        Address destination = singleOrderPoint ? boxMembers.get(0) : boxMembers.get(random.nextInt(boxMembers.size())); // Select box at random;
        Message requestMessage = new Message(destination).src(localAddress).putHeader(id, header);
        down_prot.down(new Event(Event.MSG, requestMessage));

        if (log.isTraceEnabled()) {
            try {
                MethodCall mc = (MethodCall) objectFromBuffer(message.getBuffer(), message.getOffset(), message.getLength());
                log.trace("Ordering Request Sent to " + destination + " | " + header + " | UPerf Seq := " + mc.getArgs()[0]);
            } catch(Exception e) {
                log.trace("Cast exception: " + e);
            }
        }
    }

    public Object objectFromBuffer(byte[] buffer, int offset, int length) throws Exception {
        ByteBuffer buf=ByteBuffer.wrap(buffer, offset, length);

        byte type=buf.get();
        if (type == 10) {
            Long longarg=buf.getLong();
            int len=buf.getInt();
            byte[] arg2=new byte[len];
            buf.get(arg2, 0, arg2.length);
            return new MethodCall(type, longarg, arg2);
        }
        return null;
    }

    private void handleOrderingResponse(DecoupledHeader responseHeader) {
        if (log.isTraceEnabled())
            log.trace("Ordering response received | " + responseHeader);
        // Receive the ordering list and send this message to all nodes in the destination set

        if (log.isTraceEnabled()) {
            log.trace("Ordering Response received | " + responseHeader.getMessageInfo().getOrdering());
            log.trace("Ordering received| " + responseHeader.getMessageInfo().getId() + " | dest " + Arrays.toString(responseHeader.getMessageInfo().getDestinations()) +  " | " + viewManager.getDestinations(responseHeader.getMessageInfo()));
        }

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
        if (log.isTraceEnabled())
            log.trace("Broadcast Message " + ((DecoupledHeader)message.getHeader(id)).getMessageInfo().getOrdering() +
                    " to | " + destinations + " | deliverToSelf " + deliverToSelf);

        for (Address destination : destinations) {
            if (destination.equals(localAddress))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
            if (log.isTraceEnabled())
                log.trace("Broadcast Sent := " + ((DecoupledHeader)message.getHeader(id)).getMessageInfo().getOrdering() + " | To := " + destination);
        }

        DecoupledHeader header = (DecoupledHeader)message.getHeader(id);
        if (deliverToSelf) {
            message.setDest(localAddress);
            deliveryManager.addMessageToDeliver(header, message, true);
        } else {
            messageStore.remove(header.getMessageInfo().getId());
        }
    }

    private void handleBroadcast(DecoupledHeader header, Message message) {
        if (log.isTraceEnabled())
            log.debug("Broadcast received | " + header.getMessageInfo().getOrdering() + " | Src := " + message.getSrc());
        deliveryManager.addMessageToDeliver(header, message, false);
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
            log.trace("Deliver Message | " + (DecoupledHeader)message.getHeader(this.id));

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