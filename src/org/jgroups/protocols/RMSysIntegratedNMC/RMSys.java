package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.TimeScheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
    private final int initialProbeFreq = 5; // Time between each probe during initial probe period

    @Property(name = "initial_probe_count", description = "The number of probes that should be sent by this node" +
            " before message sending can occur")
    private final int initialProbeCount = 750; // Time between each probe during initial probe period

    @Property(name = "minimum_nodes", description = "The minimum number of nodes allowed in a cluster")
    private int minimumNodes = 2;

    private final Map<MessageId, MessageRecord> messageRecords = new ConcurrentHashMap<MessageId, MessageRecord>();
    private final Map<MessageId, Message> receivedMessages = new ConcurrentHashMap<MessageId, Message>();
    private final Map<MessageId, Future> responsiveTasks = new ConcurrentHashMap<MessageId, Future>();

    private PCSynch clock = null;
    private NMC nmc = null;
    private DeliveryManager deliveryManager = null;
    private Address localAddress = null;
    private TimeScheduler timer;
    private ExecutorService executor;
    private View view;

    @Override
    public void init() throws Exception {
        log.setLevel("trace");

        timer = getTransport().getTimer();
        clock = new PCSynch();

        // TODO change so that Events are passed up and down the stack (temporary hack)
        getProtocolStack().insertProtocol(clock, ProtocolStack.BELOW, this.getName());

        nmc = new NMC(clock);
        deliveryManager = new DeliveryManager(nmc);
    }

    @Override
    public void start() throws Exception {
        super.start();
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new DeliverMessages());
        timer.scheduleWithDynamicInterval(new ProbeScheduler());
    }

    @Override
    public void stop() {
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
                view = (View) event.getArg();

                if (log.isDebugEnabled())
                    log.debug("View Change | new view := " + view);

                // TODO How to make this associated with just box members
                // Use a seperate view for those involved with RMSys i.e. boxMembers
                nmc.setActiveNodes(view.size());
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

    public void deliver(Message message) {
        message.setDest(localAddress);

        if (log.isTraceEnabled()) {
            log.trace("Deliver message " + message + " in total order");
        }

        up_prot.up(new Event(Event.MSG, message));
    }

    public void sendRMCast(Message message) {
        NMCData data = nmc.getData();

        Collection<Address> destinations = view.getMembers();
        if (message.getDest() == null)
            message.setDest(new AnycastAddress(destinations)); // If null must set to Anycast Address

        MessageId messageId = new MessageId(clock.getTime(), localAddress);
        message.putHeader(id, RMCastHeader.createBroadcastHeader(messageId, localAddress, 0, data, destinations));
        timer.execute(new MessageBroadcaster(message, 0, data.getMessageCopies(), data.getEta(), id));

        if (log.isTraceEnabled())
            log.trace("RMCast Sent := " + messageId);
    }

    public void handleMessage(Event event) {
        Message message = (Message) event.getArg();
        RMCastHeader header = (RMCastHeader) message.getHeader(id);
        handleMessage(event, header);
    }

    public void handleMessage(Event event, RMCastHeader header) {
        recordProbe(header); // Record probe latency

        if (header.getType() == RMCastHeader.EMPTY_PROBE_MESSAGE)
            return; // No need to RMCast empty probe messages as we only want the latency

        if (log.isTraceEnabled())
            log.trace("Message received | " + header);

        final MessageRecord record;
        MessageRecord newRecord = new MessageRecord(header);
        MessageRecord oldRecord = (MessageRecord) ((ConcurrentHashMap) messageRecords).putIfAbsent(header.getId(), newRecord);
        if (oldRecord == null) {
            up_prot.up(event); // Deliver to the above layer if this is the first time RMCast has received M
            record = newRecord;
            receivedMessages.put(header.getId(), (Message) event.getArg()); // Store actual message, need for retransmission
        } else {
            record = oldRecord;
        }
        handleRMCastCopies(header, record);
    }

    private void recordProbe(RMCastHeader header) {
        Address originator = header.getId().getOriginator();
        if (!clock.isSynchronised() || originator.equals(localAddress) || header.getCopy() > 0)
            return;

        // Only record probe information if the message has come directly from its source i.e not disseminated
        if (header.getDisseminator().equals(originator)) {
            nmc.receiveProbe(header); // Record probe information piggybacked on this message
        }
    }

    private void handleRMCastCopies(RMCastHeader header, MessageRecord record) {
        if (record.largestCopyReceived == header.getCopyTotal() && record.crashNotified) {
            // Cancel any responsiveness tasks that belong to this message and remove the record
            Future f = responsiveTasks.get(header.getId());
            if (f != null)
                f.cancel(true);

            receivedMessages.remove(header.getId());
            return;
        }

        if (header.getDisseminator().equals(header.getId().getOriginator()) && !record.crashNotified) {
            // TODO SEND NO CRASH NOTIFICATION
            // Effectively cancels the CrashedTimeout as nothing will happen once this is set to true
            record.crashNotified = true;
        }

        if (header.getCopy() == header.getCopyTotal()) {
            record.largestCopyReceived = header.getCopy();
        } else {
            if (!header.getId().getOriginator().equals(localAddress)
                    && header.getCopy() > record.largestCopyReceived
                    || (header.getCopy() == record.largestCopyReceived && (header.getDisseminator().equals(header
                    .getId().getOriginator()) || header.getDisseminator().compareTo(record.broadcastLeader) < 0))) {

                if (log.isTraceEnabled())
                    log.trace("Starting responsiveness timeout for message := " + header.getId());

                responsivenessTimeout(record, header);
            }
        }
    }

    private void responsivenessTimeout(final MessageRecord record, final RMCastHeader header) {
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
            Future task = timer.schedule(new Runnable() {
                @Override
                public void run() {
                    if (record.largestCopyReceived < header.getCopyTotal()) {
                        record.broadcastLeader = null;
                        Random r = new Random();
                        int eta = data.getEta();
                        final int ran = r.nextInt(eta < 1 ? eta + 1 : eta) + 1; // +1 makes 0 exclusive and eta inclusive

                        Future nextTask = timer.schedule(new Runnable() {
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
                        }, ran, TimeUnit.MILLISECONDS);
                        // Set the responsivenes task to be this newTask.
                        // Allows this newTask to be cancelled if this method is called by a subsequent message copy
                        responsiveTasks.put(record.id, nextTask);
                    }
                }
            }, timeout, TimeUnit.MILLISECONDS);
            // Store this task so that it can be used by the above if statement when the method is called again
            responsiveTasks.put(record.id, task);
        }
    }

    private void broadcastMessage(Message message) {
        if (!(message.getDest() instanceof AnycastAddress))
            throw new IllegalArgumentException("A messages destination must be an AnycastAddress");

        if (log.isTraceEnabled())
            log.trace("Broadcast Message := " + message.getHeader(id));

        message.setSrc(localAddress);
        AnycastAddress address = (AnycastAddress) message.getDest();
        for (Address destination : address.getAddresses()) {
            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            if (!destination.equals(localAddress))
                down_prot.down(new Event(Event.MSG, messageCopy));
            else
                handleMessage(new Event(Event.MSG, messageCopy));
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
        private int numberOfProbesSent = 0;

        @Override
        public void run() {
            if (view == null || view.size() < minimumNodes || !clock.isSynchronised())
                return;

            log.trace("Sending empty probe messsage | Clock synch := " + clock.isSynchronised());
            timer.execute(createEmptyProbeMessage()); // Send empty probe messages
            numberOfProbesSent++;

            if (!initialProbesReceived && nmc.initialProbesReceived()) {
                if (log.isDebugEnabled())
                    log.debug("Initial Probes Received!");
                initialProbesReceived = true;
            }

            if (log.isDebugEnabled() && numberOfProbesSent >= initialProbeCount)
                log.debug("Initial probes sent");
        }

        public MessageBroadcaster createEmptyProbeMessage() {
            Collection<Address> destinations = view.getMembers(); // TODO change so this is just box members? Not all view members
            MessageId messageId = new MessageId(clock.getTime(), localAddress);
            Header header = RMCastHeader.createEmptyProbeHeader(messageId, localAddress, nmc.getData(), destinations);
            Message message = new Message().putHeader(id, header);
            message.setDest(new AnycastAddress(destinations));
            return new MessageBroadcaster(message, 0, 0, 0, id);
        }

        public long nextInterval() {
            return numberOfProbesSent >= initialProbeCount && initialProbesReceived ? 0 : initialProbeFreq;
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

            message.setDest(new AnycastAddress(header.getDestinations()));
            broadcastMessage(message);
            // Update to show that the largestCopy received == last broadcast i.e we're the disseminator
            record.largestCopyReceived = messageCopy;
            record.lastBroadcast = messageCopy;
            executeAgain();
        }

        // We use this instead of timer.scheduleWithDynamicInterval because using timer.execute() executes the initial task
        // 6 times faster than using DynamicInterval.
        public void executeAgain() {
            if (record.largestCopyReceived < header.getCopyTotal() && record.broadcastLeader.equals(localAddress))
                timer.schedule(new MessageDisseminator(record, header, delay), delay, TimeUnit.MILLISECONDS);
        }
    }

    final class MessageRecord {
        final private MessageId id;
        final private int totalCopies;
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