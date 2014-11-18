package org.jgroups.protocols.abaas;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.aramis.PCSynch;
import org.jgroups.protocols.aramis.Aramis;
import org.jgroups.protocols.tom.TOA;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abaas protocol used to provide total ordering as an external service.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class AbaaS extends Protocol {

    public static int minimumNodes = 2; // Static hack to allow experiments to dynamically change the value.

    @Property(name = "box_member", description = "Is this node a box member")
    private boolean boxMember = false;

    @Property(name = "box_members", description = "A list of hostnames that will be box members (seperated by a colon)")
    private String boxHostnames = "";

    @Property(name = "total_order", description = "The name of the total order protocol to be used by box members")
    private String totalOrderProtocol = "TOA";

    @Property(name = "msg_size", description = "The max size of a msg between box members.  Determines the number of msgs that can be bundled")
    private int MSG_SIZE = 1000;

    @Property(name = "queue_capacity", description = "The maximum number of ordering requests that can be queued")
    private int QUEUE_CAPACITY = 500;

    @Property(name = "bundle_msgs", description = "If true then ordering requests will be bundled when possible" +
            "in order to reduce the number of total order broadcasts between box members")
    private boolean BUNDLE_MSGS = true;

    @Property(name = "profile", description = "If true the profiler will record statistics about this protocol")
    private boolean PROFILE_ENABLED = true;

    private Profiler profiler = new Profiler(PROFILE_ENABLED);
    private Address localAddress = null;
    private final ViewManager viewManager = new ViewManager();
    private final DeliveryManager deliveryManager = new DeliveryManager(log, viewManager);
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private final BlockingQueue<AbaaSHeader> inputQueue = new ArrayBlockingQueue<AbaaSHeader>(QUEUE_CAPACITY);
    private final List<Address> boxMembers = new ArrayList<Address>();
    private View view = null;
    private OrderingBox box;
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private Random random = new Random(); // Random object for selecting which box member to use
    private ExecutorService executor;
    // If true then ordering requests will only be sent to one box member, otherwise all box members are used
    private final boolean singleOrderPoint = false;

    public AbaaS() {
    }

    private void logHack() {
        Logger logger = Logger.getLogger(this.getClass().getName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    @Override
    public void init() throws Exception {
//        logHack();
//        setLevel("info");

        if (boxMember) {
            createProtocolStack();
            getTransport().getTimer().schedule(new BoxMemberAnnouncement(), 20, TimeUnit.SECONDS);

            // Must be after the protocols have been added to the stack to ensure that down_prot is set to the correct protocol
            box = new OrderingBox(id, log, down_prot, viewManager, boxMembers, profiler);

            if (PROFILE_ENABLED) {
                // TODO remove
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("AbaaS -------\n" + profiler);
                    }
                }));
            }
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

            PCSynch clock = new PCSynch(minimumNodes, hostnames);
            Aramis aramis = new Aramis(clock, hostnames);

            // TODO change so that Events are passed up and down the stack (temporary hack)
            getProtocolStack().insertProtocol(clock, ProtocolStack.BELOW, this.getName());
            getProtocolStack().insertProtocol(aramis, ProtocolStack.BELOW, this.getName());

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
        executor.execute(new MessageHandler());
    }

    @Override
    public void stop() {
        if (log.isDebugEnabled())
            log.debug(profiler.toString());

        executor.shutdown();
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                AbaaSHeader header = (AbaaSHeader) message.getHeader(id);
                if (header == null)
                    break;

                switch (header.getType()) {
                    case AbaaSHeader.BOX_MEMBER:
                        boxMembers.add(message.getSrc());
                        if (log.isInfoEnabled())
                            log.info("Box Member discovered | " + message.getSrc());
                        break;
                    case AbaaSHeader.BOX_REQUEST:
                        handleOrderingRequest(header);
                        break;
                    case AbaaSHeader.BOX_ORDERING:
                        box.receiveOrdering(header, message);
                        break;
                    case AbaaSHeader.BUNDLED_MESSAGE:
                        box.receiveMultipleOrderings(header, message);
                        break;
                    case AbaaSHeader.BOX_RESPONSE:
                        handleOrderingResponse(header);
                        break;
                    case AbaaSHeader.BROADCAST:
                        handleBroadcast(header, message);
                        break;
                    case AbaaSHeader.SINGLE_DESTINATION:
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

    private void handleOrderingRequest(AbaaSHeader header) {
        try {
            log.trace("Request received | " + header.getMessageInfo().getId());
            inputQueue.add(header);
        } catch (IllegalStateException e) {
            if (log.isErrorEnabled())
                log.error("Ordering Request rejected : " + e);
        }
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
            message.putHeader(id, AbaaSHeader.createSingleDestination(messageInfo));
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
        AbaaSHeader header = AbaaSHeader.createBoxRequest(messageInfo);

        Address destination = singleOrderPoint ? boxMembers.get(0) : boxMembers.get(random.nextInt(boxMembers.size())); // Select box at random;
        Message requestMessage = new Message(destination).src(localAddress).putHeader(id, header);
        down_prot.down(new Event(Event.MSG, requestMessage));
    }

    private void handleOrderingResponse(AbaaSHeader responseHeader) {
        if (log.isTraceEnabled())
            log.trace("Ordering response received | " + responseHeader);

        MessageInfo messageInfo = responseHeader.getMessageInfo();
        AbaaSHeader header = AbaaSHeader.createBroadcast(messageInfo);
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
            log.trace("Broadcast Message " + ((AbaaSHeader) message.getHeader(id)).getMessageInfo().getOrdering() +
                    " to | " + destinations + " | deliverToSelf " + deliverToSelf);

        for (Address destination : destinations) {
            if (destination.equals(localAddress))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
        }

        AbaaSHeader header = (AbaaSHeader)message.getHeader(id);
        if (deliverToSelf) {
            message.setDest(localAddress);
            deliveryManager.addMessageToDeliver(header, message, true);
        } else {
            messageStore.remove(header.getMessageInfo().getId());
        }
    }

    private void handleBroadcast(AbaaSHeader header, Message message) {
        if (log.isTraceEnabled())
            log.trace("Broadcast received | " + header.getMessageInfo().getOrdering() + " | Src := " + message.getSrc());
        deliveryManager.addMessageToDeliver(header, message, false);
    }

    private void handleSingleDestination(Message message) {
        if (log.isTraceEnabled())
            log.trace("Single Destination Message received | " + message.getHeader(id));
        deliveryManager.addSingleDestinationMessage(message);
    }

    private void deliverMessage(Message message) {
        MessageId id = ((AbaaSHeader)message.getHeader(this.id)).getMessageInfo().getId();
        messageStore.remove(id);
        message.setDest(localAddress);

        if (log.isTraceEnabled())
            log.trace("Deliver Message | " + (AbaaSHeader) message.getHeader(this.id));

        up_prot.up(new Event(Event.MSG, message));
    }

    final class MessageHandler implements Runnable {
        @Override
        public void run() {
            if (boxMember)
                handleRequests();
            else
                deliverMessages();
        }

        private void handleRequests() {
            while (true) {
                try {
                    AbaaSHeader header = inputQueue.take();
                    if (BUNDLE_MSGS) {
                        int bundleSize = 0;
                        if (inputQueue.isEmpty()) {
                            box.handleOrderingRequest(header.getMessageInfo());
                        } else {
                            List<MessageInfo> requests = new ArrayList<MessageInfo>();
                            requests.add(header.getMessageInfo());
                            while ((header = inputQueue.peek()) != null && (bundleSize + header.size()) < MSG_SIZE) {
                                inputQueue.poll(); // Just removes header. Peek necessary because of the size check
                                MessageInfo info = header.getMessageInfo();
                                bundleSize += info.size();
                                requests.add(info);
                            }
                            box.handleOrderingRequests(requests);
                        }
                    } else {
                        box.handleOrderingRequest(header.getMessageInfo());
                    }

                } catch (InterruptedException e) {
                    if (log.isErrorEnabled())
                        log.error("AbaaS:handleRequests():" + e);
                    break;
                }
            }
        }

        private void deliverMessages() {
            while (true) {
                try {
                    List<Message> messages = deliveryManager.getNextMessagesToDeliver();
                    for (Message message : messages) {
                        try {
                            deliverMessage(message);
                        } catch (Throwable t) {
                            if (log.isWarnEnabled())
                                log.warn("AbaaS: Exception caught while delivering message " + message + ":" + t.getMessage());
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
            final Message message = new Message(null, localAddress, new byte[0]).putHeader(id, AbaaSHeader.createBoxMember());
            Event event = new Event(Event.MSG, message);
            down_prot.down(event);

            if (log.isInfoEnabled())
                log.info("I am Box Message Sent | " + message);
        }
    }
}