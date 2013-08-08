package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Tuple;
import org.jgroups.util.UUID;

import java.util.*;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
@MBean
public class NMC extends Protocol {

    @Property(description="Number of additional ports to be probed for membership. A port_range of 0 does not " +
            "probe additional ports. Example: initial_hosts=A[7800] port_range=0 probes A:7800, port_range=1 probes " +
            "A:7800 and A:7801")
    private int port_range=1;

    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership",
            converter=PropertyConverters.InitialHosts.class, dependsUpon="port_range",
            systemProperty= Global.TCPPING_INITIAL_HOSTS)
    private List<IpAddress> initial_hosts= Collections.EMPTY_LIST;

    @Property(name="direct_latency_probability", description="The probability of a message reaching all " +
            "destinations within the calculated period.  Cannot be 1")
    private double directLatencyProb = 0.9999;

    @Property(name="reliability_probability", description="The probability of a message reaching all " +
            "destinations")
    private double reliabilityProb = 0.9999;

    @Property(name="minimum_probe_frequency", description="The smallest time allowed between each probing round i.e. " +
            "the minimum timeout between all nodes being probed by this node (In milliseconds)")
    private int minProbeFreq = 10000;

    @Property(name="initial_probe_frequency", description="The amount of time in milliseconds between each round of " +
             "probing until the defined set of initial probes have been received")
    private int initialProbeFreq = 500;

    @Property(name="initial_probe_no", description="The minimum number of probes that must have been received by this node")
    private int numInitProbes = 10;

    @Property(name="minimum_nodes", description="The minimum number of nodes allowed in a cluster")
    private int minimumNodes = 2;

    private final ResponseTimes responseTimes = new ResponseTimes(minProbeFreq, directLatencyProb);
    private final GuaranteeCalculator guarantees = new GuaranteeCalculator();
    private final List<Address> members = new ArrayList<Address>(11);
    private Address localAddress = null;
    private View view = null;
    private Collection<PhysicalAddress> clusterMembers;
    private boolean isLeaving = false;
    private String groupAddress = null;

    private boolean initialProbesReceived = false;

    public NMC() {
    }

    @Override
    public void init() throws Exception{
        System.out.println("NMC");
        clusterMembers = new ArrayList<PhysicalAddress>();

        TimeScheduler timer = getTransport().getTimer();
        timer.scheduleWithDynamicInterval(new ProbeScheduler());
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
                ProbeHeader header = (ProbeHeader) message.getHeader(this.id);

                if (header == null)
                    return up_prot.up(event);

                ProbeData probeData = header.getData();

                if (message.getSrc().equals(localAddress)) {
                    return null;
                }

                switch (header.getType()) {
                    case ProbeHeader.PROBE_REQ:
                        handleRequest(probeData, message.getSrc());
                        return null;
                    case ProbeHeader.PROBE_RSP:
                        handleResponse(probeData, message.getSrc());
                        return null;
                    default:
                        return null;
                }
        }

        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {

            case Event.MSG:
                return down_prot.down(event);

            case Event.FIND_INITIAL_MBRS:
                // TODO insert code to ensure that sufficient latencies have been recorded for each node
                // Once received, return initial members
            case Event.FIND_ALL_VIEWS:
                // Just return this node's view as all nodes will eventually receive the same view due to probing
                if (view != null) {
                    return new View(view.getCreator(), view.getViewId().getId(), view.getMembers());
                }
                return down_prot.down(event);

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                updateView((View) event.getArg());
                return down_prot.down(event);

            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                down_prot.down(event);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                addMember(localAddress);

                List<Address> addresses = new ArrayList<Address>();
                addresses.add(localAddress);
                view = new View(localAddress, System.nanoTime(), new ArrayList<Address>(addresses));

                System.out.println("LOCAL_ADDRESS := " + localAddress);
                return down_prot.down(event);

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                isLeaving = false;
                groupAddress = (String) event.getArg();
                System.out.println("Group := " + groupAddress);
                return down_prot.down(event);
            // TODO HANDLE CONNECT

            case Event.DISCONNECT:
                isLeaving=true;
                // TODO HANDLE DISCONNECT
                return down_prot.down(event);

            default:
                return down_prot.down(event);
        }
    }

    public void sendProbes(String clusterName) {
        PhysicalAddress physicalAddress = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, localAddress));
        ProbeData data = new ProbeData(localAddress, view, UUID.get(localAddress), Arrays.asList(physicalAddress), 0, System.nanoTime());

        ProbeHeader header = new ProbeHeader(ProbeHeader.PROBE_REQ, data, clusterName);
        Collection<Address> clusterMembers = fetchClusterMembers(clusterName);

        if (clusterMembers == null) {
            multicastProbes(data, header);
        }
        else {
            unicastProbes(clusterMembers, data, header);
        }
    }

    public void multicastProbes(ProbeData data, ProbeHeader header) {
        final Message message = new Message(null);
        message.setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);
        message.putHeader(this.id, header);

        for (Address address : members) {
            if(!localAddress.equals(address))
                responseTimes.addSentProbe(address, data);
        }

        down_prot.down(new Event(Event.MSG, message));
    }

    public void unicastProbes(Collection<Address> destinations, ProbeData data, ProbeHeader header) {
        for (final Address address : destinations) {
            if (address.equals(localAddress)) {
                continue;
            }

            final Message message = new Message(address);
            message.setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);
            message.putHeader(this.id, header);

            responseTimes.addSentProbe(address, data);
            down_prot.down(new Event(Event.MSG, message));
        }
    }

    public void handleRequest(ProbeData probeData, Address messageSource) {
        saveAddresses(probeData, messageSource);
        updateView(probeData.getView());
        responseTimes.receiveXMax(probeData);

        PhysicalAddress address = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, localAddress));
        Collection<PhysicalAddress> physicalAddresses = Arrays.asList(address);
        probeData = new ProbeData(localAddress, view, UUID.get(localAddress), physicalAddresses, responseTimes.getXMax(), probeData.getTimeSent());

        final ProbeHeader header = new ProbeHeader(ProbeHeader.PROBE_RSP, probeData);
        final Message response = new Message(messageSource).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE).putHeader(this.id, header);

        down_prot.down(new Event(Event.MSG, response));
    }

    public void handleResponse(ProbeData probeData, Address messageSource) {
        saveAddresses(probeData, messageSource);
        updateView(probeData.getView());

        responseTimes.receiveProbe(probeData);
    }

    public void saveAddresses(ProbeData probeData, Address messageSource) {
        if (probeData != null) {
            Address logicalAddress = probeData.getAddress();
            if (logicalAddress == null)
                logicalAddress = messageSource;

            Collection<PhysicalAddress> physicalAddresses = probeData.getPhysicalAddrs();
            PhysicalAddress sourcePhysical = null;
            if (physicalAddresses != null && !physicalAddresses.isEmpty())
                sourcePhysical = physicalAddresses.iterator().next();

            if (logicalAddress != null && probeData.getLogicalName() != null)
                UUID.add(logicalAddress, probeData.getLogicalName());

            if (logicalAddress != null && sourcePhysical != null) {
                down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address, PhysicalAddress>(logicalAddress, sourcePhysical)));
            }
        }
    }

    public void addMember(Address newMember) {
        // Add address to members. I.e. the nodes that are part of the view
        synchronized (members) {
            if (!members.contains(newMember))
                members.add(newMember);
        }
    }

    public void updateView(View newView) {
        if (view.getVid().compareTo(newView.getVid()) < 0) {
            if (!view.getMembers().equals(newView.getMembers())) {
                synchronized(members) {
                    members.clear();
                    if(!newView.getMembers().contains(localAddress))
                        members.add(localAddress);
                    members.addAll(newView.getMembers());
                }
                view = new View(localAddress, System.nanoTime(), members);
            }
        }
    }

    // TODO Fetch initial cluster members from xml file if specified
    public Collection<Address> fetchClusterMembers(String clusterName) {
        return null;
//        view = new View(localAddress, System.nanoTime(), new ArrayList<Address>(initial_hosts));
//        return new HashSet<Address>(initial_hosts);
    }

    /**
     * Class responsible for periodically sending probes
     */
    final class ProbeScheduler implements Runnable, TimeScheduler.Task {

        public void run() {
            sendProbes("");

            if (!responseTimes.isInitialProbesReceived()) {
                if (guarantees.receiveInitialProbes()) {
                    System.out.println("INITIAL PROBES RECEIVED!");
                    guarantees.setInitialProbePeriod();
                    // TODO replace with user defined object
                    down_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.NMC_READY, view)));
                }
            }

            if (responseTimes.isInitialProbesReceived()) {
                guarantees.calculate();
            }
            responseTimes.nextProbePeriod();
        }

        public long nextInterval() {
            if (responseTimes.getNextProbePeriod() == 0)
                return initialProbeFreq;

            return responseTimes.getNextProbePeriod();
        }
    }

    /**
     * Class containing methods for calculating broadcast variables
     */
    final public class GuaranteeCalculator {

        private boolean receiveInitialProbes() {
            Map<Address, Integer> initialProbes = responseTimes.getInitialProbes();
            if (members.size() >= minimumNodes && initialProbes.keySet().size() > 0) {
                for (Address address : initialProbes.keySet()) {
                    if (initialProbes.get(address) <= 10) // TODO change 10 to a configurable value
                        return false;
                }
                responseTimes.initialProbesReceived();
                return true;
            }
            return false;
        }

        public void setInitialProbePeriod() {
            Map<Address, List<LatencyTime>> latencies = responseTimes.getAllTimes();
            LatencyTime largest = new LatencyTime(0,0);
            for (List<LatencyTime> list : latencies.values()) {
                LatencyTime l = Collections.max(list);
                if (l.compareTo(largest) > 0)
                    largest = l;
            }
            // Set the first Probe Period value in milliseconds
            responseTimes.setPotentialProbePeriod(largest.getLatency() / 2000000L);
            responseTimes.nextProbePeriod();
        }

        public void calculate() {
            // If the first NextProbePeriod has been set make calculations
            if (responseTimes.getNextProbePeriod() > 0) {
                removeStaleValues();

                int maxLatency = 0; // Largest latency encountered

                // Create cumulative array of latencies encountered
                int numberOfLatencies = 0;
                Map<Address, List<LatencyTime>> latencyTimes = responseTimes.getAllTimes();
                List<Integer> keyList = new LinkedList<Integer>();
                List<int[]> latencyList = new LinkedList<int[]>();

                for (Address key : latencyTimes.keySet()) {
                    List<LatencyTime> times = new ArrayList<LatencyTime>(latencyTimes.get(key));

                    for (LatencyTime time : times) {
                        numberOfLatencies++;

                        // Half latency time and convert to milliseconds. Rounding up is pessimistic
                        int latency = (int) Math.ceil(time.getLatency() / 2000000.0);
                        int latencyKey = (int) Math.floor(latency / 100) * 100;

                        maxLatency = latency > maxLatency ? latency : maxLatency;
                        String s = String.valueOf(latency);
                        // Get last two digits of latency
                        s = s.length() > 2 ? s.substring(s.length() - 2) : s;

                        if (keyList.contains(latencyKey)) {
                            int keyIndex = keyList.indexOf(latencyKey);
                            int[] array = latencyList.get(keyIndex);

                            array[Integer.parseInt(s)]++;
                            latencyList.set(keyIndex, array);
                        } else {
                            int[] array = new int[100];
                            array[Integer.parseInt(s)]++;

                            boolean flag = true;
                            for (int y = 0; y < keyList.size(); y++) {
                                if (keyList.get(y) > latencyKey) {
                                    keyList.add(y, latencyKey);
                                    latencyList.add(y, array);

                                    flag = false;
                                    break;
                                }
                            }

                            if (flag) {
                                keyList.add(latencyKey);
                                latencyList.add(array);
                            }
                        }
                    }
                }

                if (numberOfLatencies > 0) {
                    int d = 0;
                    int dPrime = 0;
                    int temp = 0;
                    boolean dFlag = false;
                    boolean dPrimeFlag = false;

                    CLOOP: for (int ii = 0; ii < keyList.size(); ii++) {
                        int key = keyList.get(ii);
                        int[] tempLatencies = latencyList.get(ii);

                        for (int yy = 0; yy < tempLatencies.length; yy++) {
                            if ((key + yy) > maxLatency) {
                                break CLOOP;
                            }
                            temp += tempLatencies[yy];

                            if (temp >= (int) Math.floor(numberOfLatencies / 2) && !dFlag) {
                                d = key + yy;
                                dFlag = true;
                            }

                            if (temp >= (int) Math.floor(numberOfLatencies * 0.75) && !dPrimeFlag) {
                                dPrime = key + yy;
                                dPrimeFlag = true;
                                // break; ??????
                            }
                        }
                    }

                    // Calculate Q
                    double q = calculateQ();

                    // Calculate RHO
                    int rho = calculateRho(q);
                    responseTimes.setNumberOfMessageCopies(rho);

                    // Calculate omega (w)
                    int omega = dPrime - d;
                    responseTimes.setOmega(omega);

                    // Calculate 1 - e - Np / d = 0.99
                    double eta = Math.ceil(-1 * d * Math.log(1 - 0.9999));
                    responseTimes.setEta(eta);

                    int capD = (int) Math.ceil(maxLatency + (rho * eta));
                    int capS = (int) Math.ceil(maxLatency + ((rho + 2) * eta) + omega);

                    responseTimes.setCapD(capD);
                    responseTimes.setCapS(capS);
                    responseTimes.setXMax(maxLatency);
                }
            }
        }

        private void removeStaleValues() {
            int numberOfPeriods = (int) Math.ceil(1 / (1 - directLatencyProb));
            List<Long> nextProbePeriods = responseTimes.getNextProbePeriods();

            // PREVENTS ZERO LATENCIES!
            if (nextProbePeriods.size() > numberOfPeriods) {
                long recentPast = 0;
                for (int y = nextProbePeriods.size() - 1; y > -1 && y > (nextProbePeriods.size() - 1 - numberOfPeriods); y--) {
                    recentPast += nextProbePeriods.get(y);
                }

                responseTimes.removeStaleLatencies(recentPast);
                responseTimes.removeStaleProbes(recentPast);
            }
        }

        private double calculateQ() {
            // Check to see if a probe has been lost
            responseTimes.discoverLostProbes();

            List<ProbeRecord> probes = responseTimes.getProbeRecords();
            double q = 0.0;

            if (probes.size() > 0) {
                int lostProbes = 0;
                for (ProbeRecord probeRecord : probes) {
                    if (probeRecord.isLost())
                        lostProbes++;
                }

                // Calculate Q
                q = (double) lostProbes / probes.size();
                responseTimes.setQ(q);
            }
            return q;
        }

        // Calculated based upon this nodes notion of view size
        private int calculateRho(double q) {
            int numberOfNodes = members.size();

            // S RHO
            int rhoS = 0;
            double rhoProbability = 0.0;
            while (rhoProbability < reliabilityProb && rhoProbability <= 1.0) {
                rhoS++;
                double x = 1 - Math.pow(q, rhoS + 2);
                rhoProbability = Math.pow(x, numberOfNodes - 2);
            }

            // D RHO
            int rhoD = 0;
            double rhoProbability1 = 0.0;
            while (rhoProbability1 < reliabilityProb && rhoProbability1 <= 1.0) {
                rhoD++;
                double x = 1 - Math.pow(q, rhoD + 1);
                rhoProbability1 = Math.pow(x, numberOfNodes - 1);
            }
            return Math.max(rhoS, rhoD);
        }
    }
}