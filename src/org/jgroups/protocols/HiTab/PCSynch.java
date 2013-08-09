package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.concurrent.TimeUnit;

/**
 * Probabilistic clock synchronisation
 *
 * @author ryan
 * @since 4.0
 */
public class PCSynch extends Protocol {

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
    private long maxLatency = 1000000; // U

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
    private boolean synchronised;
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

                PCSynchData data  = header.getData();
                switch (header.getType()) {
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
        final Message response = new Message(rspData.getSlave()).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE).putHeader(id, rspHeader);
        down_prot.down(new Event(Event.MSG, response));
    }

    private void handleResponse(PCSynchData data) {
        long timeReceived = clock.getTime();
        long D = (Math.abs(data.getRequestTime() - timeReceived)) / 2;

        // Only proceed if the half trip time is less than the maximum allowed latency.
        // Ensures that the synchronisation is accurate enough
        if (D < maxLatency) {
            attemptSucceed = true;
            final long M = data.getRequestTime() + D * (1 + 2 * rho);
            clock.startSynchronisation(clockAdjustmentTime, M);
            timer.schedule(new Runnable() {
                public void run() {
                    clock.finishSynchronisation(clockAdjustmentTime, M);
                    synchronised = true;
                }
            }, clockAdjustmentTime, TimeUnit.MILLISECONDS);
        }
    }

    final public class RequestSender implements Runnable {
        public void run() {
            long startTime = clock.getTime();
            int messagesSent = 0;
            master = view.getMembers().get(0); // Need to make this consistent for all nodes, when dynamic discovery is used

            // If this node is the master then no need to synch
            if (!localAddress.equals(master)) {
                newSynchAttempt(); // Reset booleans
                while (messagesSent < maxSynchMessages && !synchronised) {
                    sendRequest(master);
                    try {
                        Thread.sleep(attemptDuration);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  // TODO: Customise this generated block
                    }
                    messagesSent++;
                }
            } else {
                System.out.println("Do nothing, I'm the master");
                return;
            }

            if (synchronised) {
                System.out.println("Synch succeeded");
                Event event = new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.CLOCK_SYNCHRONISED));
                up_prot.up(event);
                // down_prot.down(event);
            } else {
                System.out.println("message limit reached");
            }
        }

        public void sendRequest(Address master) {
            final PCSynchData data = new PCSynchData(localAddress, clock.getTime());
            final PCSynchHeader header = new PCSynchHeader(PCSynchHeader.SYNCH_REQ, data);
            final Message response = new Message(master).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE).putHeader(id, header);
            down_prot.down(new Event(Event.MSG, response));
        }

        public void newSynchAttempt() {
            synchronised = false;
            attemptSucceed = false;
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