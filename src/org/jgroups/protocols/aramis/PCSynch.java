package org.jgroups.protocols.aramis;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Clock synchronisation protocol based upon F.Cristains work on probabilistic clock synchronisation.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class PCSynch extends Protocol {

    @Property(name = "synch_frequency", description = "The amount of time between each round of clock synchronisation" +
            "Specificed in minutes")
    private long synchFrequency = 15;

    @Property(name = "attempt_duration", description = "The maximum amount of time between each clock synch request " +
            "Specified in milliseconds")
    private long attemptDuration = 100;

    @Property(name = "clock_adjustment", description = "Time which takes the clock’s adjustment, in milliseconds")
    private long clockAdjustmentTime = 1; // In milliseconds // alpha

    @Property(name = "max_latency", description = "2 ∗ U is the maximum round trip delay allowed, hence U − min" +
            "is the maximum error done by reading the clock of another slave. In nanoseconds")
    private int maxLatency = 1000000; // U

    @Property(name = "max_synch_messages", description = "the number of messages sent to synchronise. The higher this " +
            "number is, the higher the probability of success is but in the other hand if you send too many messages " +
            "the network can be overloaded. Sending a message is a try of synchronisation. Note that if a try has " +
            "succeed, no messages are sent anymore before the next attempt.")
    private int maxSynchMessages = 100;

    private int minimumNodes = 2;

    private boolean allNodesSynched = true;
    private List<String> synchedHostnames;
    private List<Address> synchMembers;
    private View view;
    private VirtualClock clock;
    private Address master;
    private Address localAddress;
    private TimeScheduler timer;
    private volatile boolean synchronised = false;
    private volatile boolean synchInProgress = false;

    public PCSynch() {
    }

    public PCSynch(int minimumNodes) {
        this.minimumNodes = minimumNodes;
    }

    public PCSynch(int minimumNodes, List<String> synchedHostnames) {
        this.minimumNodes = minimumNodes;
        this.synchedHostnames = synchedHostnames;
        synchMembers = new ArrayList<Address>();
        allNodesSynched = false;
        if (view != null && view.size() > 0) {
            for (Address node : view.getMembers())
                for (String hostname : synchedHostnames)
                    if (node.toString().contains(hostname))
                        synchMembers.add(node);
        }
    }

    @Override
    public void init() throws Exception {
    }

    public void startClockSynch() {
        log.setLevel("info");

        clock = new VirtualClock();
        timer = getTransport().getTimer();
        timer.scheduleAtFixedRate(new RequestSender(), 0, synchFrequency, TimeUnit.MINUTES);
    }

    @Override
    public void start() throws Exception {
        super.start();
        startClockSynch();
    }

    @Override
    public void stop() {
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                PCSynchHeader header = (PCSynchHeader) message.getHeader(this.id);
                if (header == null)
                    return up_prot.up(event);

                PCSynchData data = header.data;
                switch (header.type) {
                    case PCSynchHeader.SYNCH_REQ:
                        if (log.isTraceEnabled())
                            log.trace("SYNCH_REQ received " + header);

                        sendResponse(data);
                        break;
                    case PCSynchHeader.SYNCH_RSP:
                        handleResponse(data);
                        if (log.isTraceEnabled())
                            log.trace("SYNCH_RSP received " + header);
                        break;
                }
                return null;
            case Event.VIEW_CHANGE:
                View oldView = view;
                view = (View) event.getArg();
                if (allNodesSynched) {
                    synchMembers = view.getMembers();
                } else if (oldView == null || oldView.size() < view.size()) { // Node added to view
                    for (Address node : view.getMembers())
                        for (String hostname : synchedHostnames)
                            if (node.toString().contains(hostname))
                                synchMembers.add(node);
                } else { // Node removed from view
                    for (Address member : synchMembers)
                        if (!view.getMembers().contains(member))
                            synchMembers.remove(member);
                }
                return up_prot.up(event);
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);
        }
        return down_prot.down(event);
    }

    public long getTime() {
        return clock.getTime();
    }

    public long getMaximumError() {
        return maxLatency;
    }

    public boolean isSynchronised() {
        return synchronised;
    }

    private Message generateMessage(Address destination, Header header) {
        Message message = new Message(destination);
        message.setFlag(Message.Flag.OOB)
                .putHeader(id, header)
                .setSrc(localAddress);
        return message;
    }

    private void sendResponse(PCSynchData data) {
        PCSynchData rspData = new PCSynchData(localAddress, clock.getTime(), data);
        PCSynchHeader rspHeader = new PCSynchHeader(PCSynchHeader.SYNCH_RSP, rspData);
        Message response = generateMessage(rspData.slave, rspHeader);

        if (log.isTraceEnabled())
            log.trace("Sending response to := " + response.getDest());

        down_prot.down(new Event(Event.MSG, response));
    }

    private void handleResponse(PCSynchData data) {
        long timeReceived = clock.getTime();
        long D = (Math.abs(data.requestTime - timeReceived)) / 2;

        // Only proceed if the half trip time is less than the maximum allowed latency.
        // Ensures that the synchronisation is accurate enough
        if (D < maxLatency) {
            if (synchInProgress)
                return;

            synchInProgress = true;
            final long M = data.responseTime + D;

            clock.startSynchronisation(clockAdjustmentTime, M);
            Util.sleep(clockAdjustmentTime); // TODO is this necessary, remove clockAdjustmentTime option? Set to 1
            clock.finishSynchronisation(clockAdjustmentTime, M);

            synchronised = true;
            synchInProgress = false;

            if (log.isInfoEnabled())
                log.info("Synchronisation Succeeded | Clock difference := " + clock.getDifference());
        }
    }

    final public class RequestSender implements Runnable {
        public void run() {
            // TODO can we improve this????
            while (view == null || view.size() < minimumNodes || synchMembers.isEmpty()) {
                Util.sleep(1); // Don't start clockSynch until at least minimum number of nodes have joined the cluster
            }

            int messagesSent = 0;
            master = synchMembers.get(0); // Need to make this consistent for all nodes, when dynamic discovery is used

            // If this node is the master then no need to synch
            if (!localAddress.equals(master)) {
                if (log.isDebugEnabled())
                    log.debug("Attempting to synch with master " + master);
                newSynchAttempt(); // Reset booleans
                while (messagesSent < maxSynchMessages && !synchronised) {
                    sendRequest(master);
                    Util.sleep(attemptDuration);
                    messagesSent++;
                }
            } else {
                synchronised = true;
                if (log.isInfoEnabled())
                    log.info("Do nothing, I'm the master node");
                return;
            }

            if (!synchronised) {
                log.warn("Synchronisation message limit reached");
            }
        }

        private void sendRequest(Address master) {
            if (log.isTraceEnabled())
                log.trace("Sending request to master := " + master + " | down_prot := " + getDownProtocol().getName());

            PCSynchData data = new PCSynchData(localAddress, clock.getTime());
            PCSynchHeader header = new PCSynchHeader(PCSynchHeader.SYNCH_REQ, data);
            down_prot.down(new Event(Event.MSG, generateMessage(master, header)));
        }

        private void newSynchAttempt() {
            synchronised = false;
        }
    }

    protected static class PCSynchHeader extends Header {
        public static final byte SYNCH_REQ = 1;
        public static final byte SYNCH_RSP = 2;

        private byte type = 0;
        private PCSynchData data = null;
        private String clusterName = null;

        public PCSynchHeader() {
        }

        public PCSynchHeader(byte type, PCSynchData data) {
            this.type = type;
            this.data = data;
        }

        @Override
        public int size() {
            int retval = Global.BYTE_SIZE * 3; // type, data presence and cluster_name presence
            if (data != null)
                retval += data.size();
            if (clusterName != null)
                retval += clusterName.length() + 2;
            return retval;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeStreamable(data, out);
            Bits.writeString(clusterName, out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            type = in.readByte();
            data = (PCSynchData) Util.readStreamable(PCSynchData.class, in);
            clusterName = Bits.readString(in);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[PCSynchMsg: type=" + typeToString(type));
            if (data != null)
                sb.append(", arg=" + data);
            if (clusterName != null)
                sb.append(", cluster=").append(clusterName);
            sb.append(']');
            return sb.toString();
        }

        String typeToString(byte type) {
            switch (type) {
                case SYNCH_REQ:
                    return "SYNCH_REQ";
                case SYNCH_RSP:
                    return "SYNCH_RSP";
                default:
                    return "<unknown type(" + type + ")>";
            }
        }
    }

    protected static class PCSynchData implements SizeStreamable {
        private Address slave = null; // Address of the slave node trying to synch
        private long requestTime = -1; // Time that the original request was sent
        private long responseTime = -1; // Sending time of a response message

        public PCSynchData() {
        }

        PCSynchData(Address slave, long timeSent) {
            this.slave = slave;
            this.requestTime = timeSent;
        }

        PCSynchData(Address slave, long requestTime, long responseTime) {
            this(slave, requestTime);
            this.responseTime = responseTime;
        }

        PCSynchData(Address master, long responseTime, PCSynchData request) {
            this(request.slave, request.requestTime, responseTime);
        }

        @Override
        public int size() {
            return Util.size(slave) + Bits.size(requestTime) + Bits.size(responseTime);
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeAddress(slave, out);
            out.writeLong(requestTime);
            out.writeLong(responseTime);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            slave = Util.readAddress(in);
            requestTime = in.readLong();
            responseTime = in.readLong();
        }

        @Override
        public String toString() {
            return "PCSynchData{" +
                    "slave=" + slave +
                    ", requestTime=" + requestTime +
                    ", responseTime=" + responseTime +
                    '}';
        }
    }

    /**
     * Class used as a virtual clock, modifies the hardclock and allows synchronisation with an other clock.
     */
    final private class VirtualClock {
        private long n = 0;
        private long N = 0;

        public VirtualClock() {
            n = 0;
            N = 0;
        }

        public long getTime() {
            long hardTime = System.nanoTime();
            return ((n + 1) * hardTime + N);
        }

        // M is the estimate of the virtual time of the master clock at t
        public void startSynchronisation(long alpha, long m) {
            long H = System.nanoTime(); // H is the hardtime at t
            long L = (n + 1) * H + N; // L is the virtual time at t
            n = (m - L) / alpha;
            N = L - (n + 1) * H;
        }

        public void finishSynchronisation(long alpha, long m) {
            long H = System.nanoTime(); // H is the hardtime at t+alpha
            n = 0;
            N = m + alpha - H;
        }

        public long getDifference() {
            return Math.abs(System.nanoTime() - getTime());
        }
    }
}