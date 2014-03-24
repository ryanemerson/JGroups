package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RMSys, a probabilistic total order protocol
 *
 * @author ryan
 * @since 4.0
 */
final public class RMSys extends Protocol {

    @Property(name = "initial_probe_frequency", description = "The time (in milliseconds) between each probe message that is" +
            " sent during initialisation")
    private final int initialProbeFrequency = 5; // Time between each probe during initial probe period

    @Property(name = "initial_probe_count", description = "The number of probes that should be sent by this node" +
            " before message sending can occur")
    private final int initialProbeCount = 750; // Time between each probe during initial probe period

    @Property(name = "minimum_nodes", description = "The minimum number of nodes allowed in a cluster")
    private int minimumNodes = 2;

    @Property(name = "max_acks_per_message", description = "The maximum number of messages that can be acked in one message")
    private int numberOfAcks = 10;

    private final Map<MessageId, MessageRecord> messageRecords = new ConcurrentHashMap<MessageId, MessageRecord>();
    private final Map<MessageId, Message> receivedMessages = new ConcurrentHashMap<MessageId, Message>();
    private final Map<MessageId, Future> responsiveTasks = new ConcurrentHashMap<MessageId, Future>();

    private PCSynch clock = null;
    private NMC nmc = null;
    private DeliveryManager deliveryManager = null;
    private SenderManager senderManager = null;
    private Address localAddress = null;
    private TimeScheduler timer;
    private ExecutorService executor;
    private View view;
    private Future sendEmptyAckFuture;

    private boolean allNodesActive = true;
    private List<String> activeHostnames;
    private List<Address> activeMembers = new ArrayList<Address>();

    private final boolean profilingEnabled = true;
    private final Profiler profiler = new Profiler(profilingEnabled);

    public RMSys() {
    }

    public RMSys(PCSynch clock, List<String> activeHostnames) {
        this.clock = clock;
        this.activeHostnames = activeHostnames;
        allNodesActive = false;
        if (view != null && view.size() > 0) {
            for (Address node : view.getMembers())
                for (String hostname : activeHostnames)
                    if (node.toString().contains(hostname))
                        activeMembers.add(node);
        }
    }

    @Override
    public void init() throws Exception {
        if (clock == null) {
            clock = new PCSynch();

            // TODO change so that Events are passed up and down the stack (temporary hack)
            getProtocolStack().insertProtocol(clock, ProtocolStack.BELOW, this.getName());
        }
    }

    @Override
    public void start() throws Exception {
//        log.setLevel("fatal");
        if (activeMembers.size() > 0)
            nmc.setActiveNodes(activeMembers.size());

        nmc = new NMC(clock, profiler);
        deliveryManager = new DeliveryManager(this, profiler);
        senderManager = new SenderManager(clock, numberOfAcks);

        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());

        timer = getTransport().getTimer();
        timer.scheduleWithDynamicInterval(new ProbeScheduler());
    }

    @Override
    public void stop() {
        if (log.isDebugEnabled()) {
            log.debug(nmc.getData().toString());
            log.debug(profiler.toString());
            log.debug(deliveryManager.toString());
        }
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                RMCastHeader header = (RMCastHeader) message.getHeader(this.id);

                if (header == null)
                    break;

                handleMessage(event, header);
                return null;
            case Event.VIEW_CHANGE:
                View oldView = view;
                view = (View) event.getArg();
                if (allNodesActive) {
                    activeMembers = view.getMembers();
                } else if (oldView == null || oldView.size() < view.size()) { // Node added to view
                    for (Address node : view.getMembers())
                        for (String hostname : activeHostnames)
                            if (node.toString().contains(hostname))
                                activeMembers.add(node);
                } else { // Node removed from view
                    for (Address member : activeMembers)
                        if (!view.getMembers().contains(member))
                            activeMembers.remove(member);
                }

                if (log.isDebugEnabled())
                    log.debug("View Change | new view := " + view);

                // TODO How to make this associated with just box members?
                // There must be a cleaner solution then passing hostnames!
                // Use a seperate view for those involved with RMSys i.e. boxMembers
                if (nmc != null)
                    nmc.setActiveNodes(activeMembers.size());
                break;
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                if (message.getDest() instanceof AnycastAddress) {
                    sendRMCast(message);
                    return null;
                }
                break; // If not an anycast address send down

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                break;
        }
        return down_prot.down(event);
    }

    public PCSynch getClock() {
        return clock;
    }

    public Address getLocalAddress() {
        return localAddress;
    }

    public void collectGarbage(MessageId id) {
        messageRecords.remove(id);
        receivedMessages.remove(id);
        responsiveTasks.remove(id);
    }

    private void deliver(Message message) {
        message.setDest(localAddress);

        if (log.isDebugEnabled())
            log.debug("Deliver message " + message.getHeader(id));

        RMCastHeader header = (RMCastHeader) message.getHeader(this.id);
        collectGarbage(header.getId());

        profiler.messageDelivered();
        up_prot.up(new Event(Event.MSG, message));
    }

    private void sendRMCast(Message message) {
        // Stop empty ack message from being sent, unless it has already started
        if (sendEmptyAckFuture != null)
            sendEmptyAckFuture.cancel(false);

        NMCData data = nmc.getData();

        // TODO is this if necessary? Should the destination always be set to activeMembers regardless of the applications request?
        // As total order broadcasts can't be sent to subsets, only a set of nodes.
        // How would this work when a node is considered unresponsive?
        Collection<Address> destinations = activeMembers;
//        if (message.getDest() == null || !(message.getDest() instanceof AnycastAddress))
            message.setDest(new AnycastAddress(destinations)); // If null must set to Anycast Address

        // Generate message header and store locally
        RMCastHeader header = deliveryManager.addLocalMessage(senderManager, message, localAddress, data, id, destinations);
        messageRecords.put(header.getId(), new MessageRecord(header));
        receivedMessages.put(header.getId(), message); // Store actual message, need for retransmission

        // schedule message broadcast
        timer.execute(new MessageBroadcaster(message, 0, data.getMessageCopies(), data.getEta(), id));

        if (log.isDebugEnabled())
            log.debug("Broadcast MSG := " + header);
    }

    private void sendEmptyAckMessage() {
        Collection<Address> destinations = activeMembers; // TODO change so that only box members are selected
        RMCastHeader header = senderManager.newEmptyBroadcast(localAddress, destinations);
        if (header == null)
            return;

        Message message = new Message(new AnycastAddress(destinations));
        message.putHeader(this.id, header);
        broadcastMessage(message);

        if (log.isDebugEnabled())
            log.debug("Empty ack message sent ct := " + clock.getTime() +
                      " | #Acks :=  " + header.getAcks().size() + " | Acks := " + header.getAcks());

        profiler.emptyAckMessageSent();
        if (senderManager.acksRequired())
            sendEmptyAckMessage();
    }

    private void handleMessage(Event event, RMCastHeader header) {
        if (log.isTraceEnabled())
            log.trace("Message received | " + header);

        if (header.getType() == RMCastHeader.EMPTY_ACK_MESSAGE) {
            deliveryManager.processEmptyAckMessage(header);
            profiler.emptyAckMessageReceived();
        }
        // No need to RMCast empty probe messages as we only want the latency
        else if (header.getType() != RMCastHeader.EMPTY_PROBE_MESSAGE) {
            // If this headers sequence has already expired then we don't want to process it again
            if (deliveryManager.hasMessageExpired(header)) {
                if (header.getCopy() == 0)
                    recordProbe(header); // Still record the probe latency, as we want to take into account larger latencies
                return;
            }

            final MessageRecord record;
            MessageRecord newRecord = new MessageRecord(header);
            MessageRecord oldRecord = (MessageRecord) ((ConcurrentHashMap) messageRecords).putIfAbsent(header.getId(), newRecord);
            if (oldRecord == null) {
                Message message = (Message) event.getArg();
                deliveryManager.addMessage(message); // Add to the delivery manager if this is the first time RMCast has received M
                receivedMessages.put(header.getId(), message); // Store actual message, need for retransmission

                record = newRecord;
                profiler.messageReceived(header.getCopy() > 0);
            } else {
                record = oldRecord;
            }
            handleRMCastCopies(header, record);
        } else {
            profiler.emptyProbeMessageReceived();
        }
        recordProbe(header); // Record probe latency
    }

    // Schedule an emptyAckMessage to be sent after ackWait period of time
    // This should be called whenever a new message is received
    public void handleAcks(RMCastHeader header) {
        if (header.getId().getOriginator().equals(localAddress))
            return;

        senderManager.addMessageToAck(header.getId());
        NMCData nmcData = header.getNmcData();
        int ackWait = (2 * nmcData.getEta()) + nmcData.getOmega();
        // If a future is already in progress and hasn't completed, then do nothing as that future will execute sooner
        // and should send the acks that this message would have sent
        if (sendEmptyAckFuture == null || sendEmptyAckFuture.isDone()) {
            sendEmptyAckFuture = timer.schedule(new Runnable() {
                @Override
                public void run() {
                    sendEmptyAckMessage();
                }
            }, ackWait, TimeUnit.MILLISECONDS);
        }
    }

    private void recordProbe(RMCastHeader header) {
        Address originator = header.getId().getOriginator();
        if (!clock.isSynchronised() || originator.equals(localAddress) || header.getCopy() > 0)
            return;

        // Only record probe information if the message has come directly from its source i.e not disseminated
        if (header.getDisseminator().equals(originator)) {
            nmc.receiveProbe(header); // Record probe information piggybacked on this message
            profiler.probeReceieved();
        }
    }

    private void handleRMCastCopies(RMCastHeader header, MessageRecord record) {
        if (record.largestCopyReceived == header.getCopyTotal() && record.crashNotified) {
            // Cancel any responsiveness tasks that belong to this message and remove the record
            Future f = responsiveTasks.get(header.getId());
            if (f != null)
                f.cancel(true);
            responsiveTasks.remove(header.getId());
            return;
        }

        Address originator = header.getId().getOriginator();
        Address disseminator = header.getDisseminator();
        if (disseminator.equals(originator) && !record.crashNotified) {
            // TODO SEND NO CRASH NOTIFICATION
            // Effectively cancels the CrashedTimeout as nothing will happen once this is set to true
            record.crashNotified = true;
        }

        if (header.getCopy() == header.getCopyTotal()) {
            record.largestCopyReceived = header.getCopy();
        } else {
            if (!originator.equals(localAddress) && header.getCopy() > record.largestCopyReceived
                    || (header.getCopy() == record.largestCopyReceived && (disseminator.equals(originator)
                    || record.broadcastLeader == null
                    || disseminator.compareTo(record.broadcastLeader) < 0))) {

                if (log.isTraceEnabled())
                    log.trace("Starting responsiveness timeout for message := " + header.getId());

                responsivenessTimeout(record, header);
            }
        }
    }

    private void responsivenessTimeout(final MessageRecord record, final RMCastHeader header) {
        if (deliveryManager.hasMessageExpired(header))
            return;

        final NMCData data = header.getNmcData(); // Use included NMC data to ensure that the values relate to this message
        record.largestCopyReceived = header.getCopy();
        record.broadcastLeader = header.getDisseminator();
        // Set Timeout 2 for n + w
        final int timeout = data.getEta() + data.getOmega();
        // If there is already a responsiveness timeout in progress (Executing), do nothing
        // Otherwise cancel the timeout and start a new one
        Future oldTask = responsiveTasks.get(record.id);
        if (oldTask == null || oldTask.cancel(false)) {
            // Final check before creating the task ensuring another thread hasn't completed the broadcast of this message
            if (record.largestCopyReceived >= header.getCopyTotal())
                return;
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    if (record.largestCopyReceived < header.getCopyTotal()) {
                        record.broadcastLeader = null;
                        Random r = new Random();
                        int eta = data.getEta();
                        final int ran = r.nextInt(eta < 1 ? eta + 1 : eta) + 1; // +1 makes 0 exclusive and eta inclusive

                        Runnable newTask = new Runnable() {
                            public void run() {
                                // Check to see if we still need to start disseminating
                                if (record.largestCopyReceived < header.getCopyTotal()) {
                                    if (log.isTraceEnabled())
                                        log.trace("Responsiveness timeout expired ( " + (timeout + ran) + "ms) ... " +
                                                "Starting to disseminate message | " + record.id);

                                    record.broadcastLeader = localAddress;
                                    timer.execute(new MessageDisseminator(record, header, data.getEta()));
                                }
                            }
                        };
                        // Set the responsivenes task to be this newTask.
                        // Allows this newTask to be cancelled if this method is called by a subsequent message copy
                        createResponsiveTask(header, newTask, ran);
                    }
                }
            };
            // Store this task so that it can be used by the above if statement when the method is called again
            createResponsiveTask(header, r, timeout);
        }
    }

    private void createResponsiveTask(RMCastHeader header, Runnable r, int timeout) {
        if (deliveryManager.hasMessageExpired(header))
            return;

        Future task = timer.schedule(r, timeout, TimeUnit.MILLISECONDS);
        responsiveTasks.put(header.getId(), task);
    }

    private void broadcastMessage(Message message) {
        if (!(message.getDest() instanceof AnycastAddress))
            throw new IllegalArgumentException("A messages destination must be an AnycastAddress | getDest() := " + message.getDest() + " | header := " + message.getHeader(id));

        if (log.isTraceEnabled())
            log.trace("Broadcast Message := " + message.getHeader(id));

        message.setSrc(localAddress);
        AnycastAddress address = (AnycastAddress) message.getDest();
        for (Address destination : address.getAddresses()) {
            if (destination.equals(localAddress))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
        }
    }

    final class DeliverMessages implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    List<Message> deliverableMessages = deliveryManager.getDeliverableMessages();
                    for (Message message : deliverableMessages) {
                        try {
                            deliver(message);
                        } catch (Throwable t) {
                            log.error("Exception caught while delivering message " + message + ":" + t.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    final class ProbeScheduler implements Runnable, TimeScheduler.Task {
        private boolean initialProbesReceived = false;
        private boolean initialProbesSent = false;
        private int numberOfProbesSent = 0;

        @Override
        public void run() {
            if (view == null || activeMembers.size() < minimumNodes || !clock.isSynchronised())
                return;

            if (log.isTraceEnabled())
                log.trace("Sending empty probe messsage | Clock synch := " + clock.isSynchronised());
            timer.execute(createEmptyProbeMessage()); // Send empty probe messages
            numberOfProbesSent++;
            profiler.emptyProbeMessageSent();

            if (!initialProbesReceived && nmc.initialProbesReceived()) {
                if (log.isDebugEnabled())
                    log.debug("Initial Probes Received!");
                initialProbesReceived = true;
            }

            if (!initialProbesSent && numberOfProbesSent >= initialProbeCount) {
                if (log.isDebugEnabled())
                    log.debug("Initial probes sent");
                initialProbesSent = true;
            }

            if (log.isInfoEnabled() && initialProbesReceived && initialProbesSent)
                log.info("Initial probes sent and received");
        }

        public MessageBroadcaster createEmptyProbeMessage() {
            Collection<Address> destinations = activeMembers;
            MessageId messageId = new MessageId(clock.getTime(), localAddress, -1); // Sequence is not relevant hence -1
            Header header = RMCastHeader.createEmptyProbeHeader(messageId, localAddress, nmc.getData(), destinations);
            Message message = new Message().putHeader(id, header);
            message.setDest(new AnycastAddress(destinations));
            return new MessageBroadcaster(message, 0, 0, 0, id);
        }

        public long nextInterval() {
            return initialProbesSent && initialProbesReceived ? 0 : initialProbeFrequency;
        }
    }

    final class MessageBroadcaster implements Runnable {
        private final Message message;
        private final int totalCopies;
        private final int delay;
        private final short headerId;
        private final AtomicInteger currentCopy;

        public MessageBroadcaster nextCopy() {
            return new MessageBroadcaster(message, currentCopy.incrementAndGet(), totalCopies, delay, headerId);
        }

        public MessageBroadcaster(Message message, int currentCopy, int totalCopies, int delay, short headerId) {
            this.message = message;
            this.currentCopy = new AtomicInteger(currentCopy);
            this.totalCopies = totalCopies;
            this.delay = delay;
            this.headerId = headerId;
        }

        @Override
        public void run() {
            RMCastHeader header = (RMCastHeader) message.getHeader(headerId);
            header.setCopy(currentCopy.intValue());

            if (deliveryManager.hasMessageExpired(header)) {
                if (log.isDebugEnabled())
                    log.debug("Message copy not broadcast as the message has already been delivered");
                return;
            }

            if (header.getCopy() > 0)
                message.setFlag(Message.Flag.OOB); // Send copies > 0 OOB to ensure that messages aren't disseminated unnecessarily

            broadcastMessage(message);
            executeAgain();
        }

        // We use this instead of timer.scheduleWithDynamicInterval because using timer.execute() executes the initial task
        // 6 times faster than using DynamicInterval.  Thus the first message copy is sent down the stack within a sixth
        // of the time it takes with DynamicInterval.
        private void executeAgain() {
            RMCastHeader header = (RMCastHeader) message.getHeader(headerId);
            if (header.getCopy() < header.getCopyTotal())
                timer.schedule(nextCopy(), delay, TimeUnit.MILLISECONDS);
        }
    }

    final class MessageDisseminator implements Runnable {
        private final Message message;
        private final MessageRecord record;
        private final RMCastHeader header;
        private final int delay;

        public MessageDisseminator(MessageRecord record, RMCastHeader header, int delay) {
            this.record = record;
            this.header = header;
            this.delay = delay;
            this.message = receivedMessages.get(header.getId());
        }

        @Override
        public void run() {
            if (record.largestCopyReceived >= header.getCopyTotal() || message == null)
                return;

            int messageCopy = Math.max(record.lastBroadcast + 1, record.largestCopyReceived);
            header.setDisseminator(localAddress);
            header.setCopy(messageCopy);
            message.putHeader(id, header);

            if (log.isTraceEnabled())
                log.trace("Disseminating message := " + header);

            if (header.getCopy() > 0)
                message.setFlag(Message.Flag.OOB); // Send copies > 0 OOB to ensure that messages aren't disseminated unnecessarily

            message.setDest(new AnycastAddress(header.getDestinations()));
            broadcastMessage(message);
            // Update to show that the largestCopy received == last broadcast i.e we're the disseminator
            record.largestCopyReceived = messageCopy;
            record.lastBroadcast = messageCopy;
            executeAgain();
            profiler.messageDisseminated();
        }

        // We use this instead of timer.scheduleWithDynamicInterval because using timer.execute() executes the initial task
        // 6 times faster than using DynamicInterval.
        public void executeAgain() {
            if (record.largestCopyReceived < header.getCopyTotal() && record.broadcastLeader.equals(localAddress))
                timer.schedule(new MessageDisseminator(record, header, delay), delay, TimeUnit.MILLISECONDS);
        }
    }

    final class MessageRecord {
        private final MessageId id;
        private final int totalCopies;
        private volatile int largestCopyReceived; // Largest copy received
        private volatile Address broadcastLeader; // Set to null if none
        private volatile int lastBroadcast;
        private volatile boolean crashNotified; // Not currently used for anything

        public MessageRecord(RMCastHeader header) {
            this(header.getId(), header.getCopyTotal(), -1, null, -1, false, false);
        }

        public MessageRecord(MessageId id, int totalCopies) {
            this(id, totalCopies, -1, null, -1, false, false);
        }

        public MessageRecord(MessageId id, int totalCopies, int largestCopyReceived, Address broadcastLeader,
                             int lastBroadcast, boolean crashNotified, boolean ackNotified) {
            if (id == null)
                throw new IllegalArgumentException("A message records id feel cannot be null");

            this.id = id;
            this.totalCopies = totalCopies;
            this.largestCopyReceived = largestCopyReceived;
            this.broadcastLeader = broadcastLeader;
            this.lastBroadcast = lastBroadcast;
            this.crashNotified = crashNotified;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (crashNotified != that.crashNotified) return false;
            if (largestCopyReceived != that.largestCopyReceived) return false;
            if (lastBroadcast != that.lastBroadcast) return false;
            if (totalCopies != that.totalCopies) return false;
            if (broadcastLeader != null ? !broadcastLeader.equals(that.broadcastLeader) : that.broadcastLeader != null)
                return false;
            if (!id.equals(that.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + totalCopies;
            result = 31 * result + largestCopyReceived;
            result = 31 * result + (broadcastLeader != null ? broadcastLeader.hashCode() : 0);
            result = 31 * result + lastBroadcast;
            result = 31 * result + (crashNotified ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", totalCopies=" + totalCopies +
                    ", largestCopyReceived=" + largestCopyReceived +
                    ", broadcastLeader=" + broadcastLeader +
                    ", lastBroadcast=" + lastBroadcast +
                    ", crashNotified=" + crashNotified +
                    '}';
        }
    }
}