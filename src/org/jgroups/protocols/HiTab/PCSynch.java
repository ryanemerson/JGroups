package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Probabilistic clock synchronisation
 *
 * @author ryan
 * @since 4.0
 */
public class PCSynch extends Protocol {

    private AtomicInteger numberOfUdp = new AtomicInteger();
    private AtomicInteger numberOfUdp2 = new AtomicInteger();

    @Property(name = "synch_frequency", description = "The amount of time between each round of clock synchronisation" +
            "Specificed in minutes")
    private long synchFrequency = 7;

    @Property(name = "attempt_duration", description = "The maximum amount of time between each clock synch request " +
            "Specified in milliseconds")
    private long attemptDuration = 100;

    @Property(name = "clocl_adjustment", description = "Time which takes the clock’s adjustment, in milliseconds")
    private long clockAdjustmentTime = 100; // In milliseconds // alpha

    @Property(name = "max_drift_rate", description = "Maximum drift rate between a clock and real time")
    private long rho = 0;

    @Property(name = "max_latency", description = "2 ∗ U is the maximum round trip delay allowed, hence U − min" +
            "is the maximum error done by reading the clock of another slave. In nanoseconds")
    private int maxLatency = 1000000; // U

    @Property(name = "max_synch_messages", description = "the number of messages sent to synchronise. The higher this " +
            "number is, the higher the probability of success is but in the other hand if you send too many messages " +
            "the network can be overloaded. Sending a message is a try of synchronisation. Note that if a try has " +
            "succeed, no messages are sent anymore before the next attempt.")
    private int maxSynchMessages = 100;

    private View view;
    private VirtualClock clock;
    private Address master;
    private Address localAddress;
    private TimeScheduler timer;
    private volatile boolean synchronised = false;
    private volatile boolean synchInProgress = false;
    private boolean attemptSucceed;

    @Override
    public void init() throws Exception {
        System.out.println("PCSynch");
    }

    public void startClockSynch() {
        System.out.println("Start Clock Synch");
        clock = new VirtualClock();
        timer = getTransport().getTimer();
        timer.scheduleAtFixedRate(new RequestSender(), 0, synchFrequency, TimeUnit.MINUTES);
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() {
        System.out.println("PCSynch := " + numberOfUdp);
        System.out.println("PCSynch Response := " + numberOfUdp2);
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                PCSynchHeader header = (PCSynchHeader) message.getHeader(this.id);
                if (header == null)
                    return up_prot.up(event);

                if (clock == null) // Do nothing if this nodes clock has not being created yet
                    return null;

                PCSynchData data  = header.data;
                switch (header.type) {
                    case PCSynchHeader.SYNCH_REQ:
                        sendResponse(data);
                        return null;
                    case PCSynchHeader.SYNCH_RSP:
                        handleResponse(data);
                        return null;
                    default:
                        return null;
                }
            case Event.VIEW_CHANGE:
                if (synchronised) // If not synchronised then don't update as the old view is still being used
                    view = (View) event.getArg();
                return up_prot.up(event);
            case Event.USER_DEFINED:
                HiTabEvent e = (HiTabEvent) event.getArg();
                switch (e.getType()) {
                    case HiTabEvent.NMC_READY:
                        view = (View) e.getArg();
                        startClockSynch();
                        return null;
                    case HiTabEvent.GET_CLOCK_TIME:
                        return clock.getTime();
                    case HiTabEvent.GET_CLOCK_ERROR:
                        return maxLatency;
                }
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.USER_DEFINED:
                HiTabEvent e = (HiTabEvent) event.getArg();
                switch (e.getType()) {
                    case HiTabEvent.GET_CLOCK_TIME:
                        return clock.getTime();
                    case HiTabEvent.GET_CLOCK_ERROR:
                        return maxLatency;
                    default:
                        return down_prot.down(event);
                }
            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);
        }
        return down_prot.down(event);
    }

    private void sendResponse(PCSynchData data) {
        final PCSynchData rspData = new PCSynchData(localAddress, clock.getTime(), data);
        final PCSynchHeader rspHeader = new PCSynchHeader(PCSynchHeader.SYNCH_RSP, rspData);
        final Message response = new Message(rspData.slave).setFlag(Message.Flag.DONT_BUNDLE).putHeader(id, rspHeader);
        down_prot.down(new Event(Event.MSG, response));
        numberOfUdp2.incrementAndGet();
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
            attemptSucceed = true;
            final long M = data.responseTime + D;
//            final long M = data.getResponseTime() + D * (1 + 2 * rho) - 0 * rho;
            clock.startSynchronisation(clockAdjustmentTime, M);
            timer.schedule(new Runnable() {
                public void run() {
                    clock.finishSynchronisation(clockAdjustmentTime, M);
                    synchronised = true;
                    synchInProgress = false;
                }
            }, clockAdjustmentTime, TimeUnit.MILLISECONDS);
        }
    }

    final public class RequestSender implements Runnable{
        public void run() {
            int messagesSent = 0;
            master = view.getMembers().get(0); // Need to make this consistent for all nodes, when dynamic discovery is used

            // If this node is the master then no need to synch
            if (!localAddress.equals(master)) {
                newSynchAttempt(); // Reset booleans
                while (messagesSent < maxSynchMessages && !synchronised) {
                    sendRequest(master);
                    Util.sleep(attemptDuration);
                    messagesSent++;
                }
            } else {
                System.out.println("Do nothing, I'm the master");
                return;
            }

            if (synchronised) {
                System.out.println("Synch succeeded");
                System.out.println("clock diff := " + clock.getDifference());
                Event event = new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.CLOCK_SYNCHRONISED));
                up_prot.up(event);
            } else {
                System.out.println("message limit reached");
            }
            System.out.println(down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES))));
        }

        public void sendRequest(Address master) {
            final PCSynchData data = new PCSynchData(localAddress, clock.getTime());
            final PCSynchHeader header = new PCSynchHeader(PCSynchHeader.SYNCH_REQ, data);
            final Message request = new Message(master).setFlag(Message.Flag.DONT_BUNDLE).putHeader(id, header);
            down_prot.down(new Event(Event.MSG, request));
            numberOfUdp.incrementAndGet();
        }

        public void newSynchAttempt() {
            synchronised = false;
            attemptSucceed = false;
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
            if(data != null)
                retval += data.size();
            if(clusterName != null)
                retval += clusterName.length() + 2;
            return retval;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeStreamable(data, out);
            Util.writeString(clusterName, out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            type = in.readByte();
            data = (PCSynchData) Util.readStreamable(PCSynchData.class, in);
            clusterName = Util.readString(in);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[PCSynchMsg: type=" + typeToString(type));
            if(data != null)
                sb.append(", arg=" + data);
            if(clusterName != null)
                sb.append(", cluster=").append(clusterName);
            sb.append(']');
            return sb.toString();
        }

        String typeToString(byte type) {
            switch(type) {
                case SYNCH_REQ: return "SYNCH_REQ";
                case SYNCH_RSP: return "SYNCH_RSP";
                default:        return "<unknown type(" + type + ")>";
            }
        }
    }

    protected static class PCSynchData implements SizeStreamable {
        private Address slave = null; // Address of the slave node trying to synch
        private Address master = null; // Address of the master node
        private long requestTime = -1; // Time that the original request was sent
        private long responseTime = -1; // Sending time of a response message

        public PCSynchData() {
        }

        PCSynchData(Address slave, long timeSent) {
            this.slave = slave;
            this.requestTime = timeSent;
        }

        PCSynchData(Address slave, Address master, long requestTime, long responseTime) {
            this(slave, requestTime);
            this.master = master;
            this.responseTime = responseTime;
        }

        PCSynchData(Address master, long responseTime, PCSynchData request) {
            this(request.slave, master, request.requestTime, responseTime);
        }

        @Override
        public int size() {
            return Util.size(slave) + Util.size(master) + Util.size(requestTime) + Util.size(responseTime);
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeAddress(slave, out);
            Util.writeAddress(master, out);
            out.writeLong(requestTime);
            out.writeLong(responseTime);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            slave = Util.readAddress(in);
            master = Util.readAddress(in);
            requestTime = in.readLong();
            responseTime = in.readLong();
        }

        @Override
        public String toString() {
            return "PCSynchData{" +
                    "slave=" + slave +
                    ", master=" + master +
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