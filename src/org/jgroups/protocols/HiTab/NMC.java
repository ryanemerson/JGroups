package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.protocols.PingData;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class NMC extends Protocol {

    private AtomicInteger numberOfUdp = new AtomicInteger();

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
    private int minProbeFrequency = 10000;

    @Property(name="initial_probe_frequency", description="The amount of time in milliseconds between each round of " +
             "probing until the defined set of initial probes have been received")
    private int initialProbeFreq = 500;

    @Property(name="initial_probe_no", description="The minimum number of probes that must have been received by this node")
    private int numInitialProbes = 10;

    @Property(name="minimum_nodes", description="The minimum number of nodes allowed in a cluster")
    private int minimumNodes = 2;

    @Property(name = "probe_size", description =  "The minimum size of a probe message, not including its headers. " +
             "This should be >= the max message size allowed by the broadcasting protocols further up the stack")
    private int probeSize = 2048;

    private final ResponseTimes responseTimes = new ResponseTimes();
    private final GuaranteeCalculator guarantees = new GuaranteeCalculator();
    private final List<Address> members = new ArrayList<Address>(11);
    private Address localAddress = null;
    private View view = null;
    private String groupAddress = null;


    public NMC() {
    }

    @Override
    public void init() throws Exception{
        System.out.println("NMC");
        TimeScheduler timer = getTransport().getTimer();
        timer.scheduleWithDynamicInterval(new ProbeScheduler());
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() {
        System.out.println("NMC := " + numberOfUdp);
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                ProbeHeader header = (ProbeHeader) message.getHeader(this.id);
                if (header == null)
                    return up_prot.up(event);

                if (message.getSrc().equals(localAddress))
                    return null;

                ProbeData probeData = header.data;
                switch (header.type) {
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
            case Event.USER_DEFINED:
                HiTabEvent e = (HiTabEvent) event.getArg();
                switch (e.getType()) {
                    case HiTabEvent.GET_NMC_TIMES:
                        return responseTimes.nmcData;
                }
                return null;

            case Event.FIND_INITIAL_MBRS:
                // TODO insert code to ensure that sufficient latencies have been recorded for each node
                // Once received, return initial members
            case Event.FIND_ALL_VIEWS:
                // Just return this node's view as all nodes will eventually receive the same view due to probing
                if (view != null)
                    return new View(view.getCreator(), view.getViewId().getId(), view.getMembers());
                break;

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                updateView((View) event.getArg());
                break;

            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                down_prot.down(event);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                addMember(localAddress);

                List<Address> addresses = new ArrayList<Address>();
                addresses.add(localAddress);
                view = new View(localAddress, 0, new ArrayList<Address>(addresses));

                System.out.println("LOCAL_ADDRESS := " + localAddress);
                break;

            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                groupAddress = (String) event.getArg();
                System.out.println("Group := " + groupAddress);
                break;
            // TODO HANDLE CONNECT

            case Event.DISCONNECT:
                // TODO HANDLE DISCONNECT
                break;
        }
        return down_prot.down(event);
    }

    public void sendProbes(String clusterName) {
        PhysicalAddress physicalAddress = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, localAddress));
        ProbeData data = new ProbeData(localAddress, view, UUID.get(localAddress), Arrays.asList(physicalAddress), 0, System.nanoTime());

        ProbeHeader header = new ProbeHeader(ProbeHeader.PROBE_REQ, data, clusterName);
        Collection<Address> clusterMembers = fetchClusterMembers(clusterName);

        if (clusterMembers == null)
            multicastProbes(data, header);
        else
            unicastProbes(clusterMembers, data, header);
    }

    public void multicastProbes(ProbeData data, ProbeHeader header) {
        final Message message = new Message(null)
                .setFlag(Message.Flag.DONT_BUNDLE)
                .putHeader(this.id, header)
                // Create empty payload to ensure the probe is at least as large as the minimum probe size
                .setBuffer(Arrays.copyOf(new byte[0], probeSize));

        List<Address> destinations = new ArrayList<Address>(members);
        for (Address address : destinations) {
            if(!localAddress.equals(address))
                responseTimes.addSentProbe(data);
        }

        down_prot.down(new Event(Event.MSG, message));
    }

    public void unicastProbes(Collection<Address> destinations, ProbeData data, ProbeHeader header) {
        for (final Address address : destinations) {
            if (address.equals(localAddress)) {
                continue;
            }

            final Message message = new Message(address);
            message.setFlag(Message.Flag.DONT_BUNDLE);
            message.putHeader(this.id, header);

            responseTimes.addSentProbe(data);
            down_prot.down(new Event(Event.MSG, message));
        }
    }

    public void handleRequest(ProbeData probeData, Address messageSource) {
        saveAddresses(probeData, messageSource);
        updateView(probeData.getView());
        responseTimes.receiveXMax(probeData);

        PhysicalAddress address = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, localAddress));
        Collection<PhysicalAddress> physicalAddresses = Arrays.asList(address);
        probeData = new ProbeData(localAddress, view, UUID.get(localAddress), physicalAddresses, responseTimes.nmcData.getXMax(), probeData.timeSent);

        final ProbeHeader header = new ProbeHeader(ProbeHeader.PROBE_RSP, probeData);
        final Message response = new Message(messageSource)
                .setFlag(Message.Flag.DONT_BUNDLE)
                .putHeader(this.id, header)
                .setBuffer(Arrays.copyOf(new byte[0], probeSize));

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
        synchronized (view) {
            if (view.getVid().compareTo(newView.getVid()) < 0) {
                if (!view.getMembers().equals(newView.getMembers())) {
                    synchronized(members) {
                        members.clear();
                        if(!newView.getMembers().contains(localAddress))
                            members.add(localAddress);
                        members.addAll(newView.getMembers());
                    }
                    view = new View(localAddress, view.getViewId().getId() + 1, members);
                    up_prot.up(new Event((Event.VIEW_CHANGE), view));
                }
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
            numberOfUdp.incrementAndGet();
            if (!responseTimes.initialProbesReceived) {
                if (guarantees.receiveInitialProbes()) {
                    System.out.println("INITIAL PROBES RECEIVED!");
                    guarantees.setInitialProbePeriod();
                    down_prot.up(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.NMC_READY, view)));
                }
            }

            if (responseTimes.initialProbesReceived)
                guarantees.calculate();
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
                    if (initialProbes.get(address) <= numInitialProbes)
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
                List<LatencyTime> newList = new ArrayList<LatencyTime>(list);
                LatencyTime l = Collections.max(newList);
                if (l.compareTo(largest) > 0)
                    largest = l;
            }
            // Set the first Probe Period value in milliseconds
            responseTimes.potentialProbePeriod = largest.latency / 2000000L;
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
                        int latency = (int) Math.ceil(time.latency / 2000000.0);
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

                    CLOOP: for (int ii = 0; ii < keyList.size(); ii++) {
                        int key = keyList.get(ii);
                        int[] tempLatencies = latencyList.get(ii);

                        for (int yy = 0; yy < tempLatencies.length; yy++) {
                            if ((key + yy) > maxLatency) {
                                break CLOOP;
                            }
                            temp += tempLatencies[yy];
                            if (temp >= numberOfLatencies * 0.5 && !dFlag) {
                                d = key + yy;
                                dFlag = true;
                            }
                            if (temp >= numberOfLatencies * 0.75) {
                                dPrime = key + yy;
                                break CLOOP;
                            }

                        }
                    }
                    double q = calculateQ();
                    int rho = calculateRho(q);
                    int omega = dPrime - d;
    
                    double eta = Math.ceil(-1 * d * Math.log(1 - 0.99)); // Calculate 1 - e - Np / d = 0.99
                    int capD = (int) Math.ceil(maxLatency + (rho * eta));
                    int capS = (int) Math.ceil(maxLatency + ((rho + 2) * eta) + omega);
                    responseTimes.nmcData = new NMCData(eta, rho, omega, capD, capS, maxLatency);
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
            responseTimes.discoverLostProbes();
            List<ProbeRecord> probes = responseTimes.getProbeRecords();
            double q = 0.0;

            if (probes.size() > 0) {
                int lostProbes = 0;
                for (ProbeRecord probeRecord : probes) {
                    if (probeRecord.isLost)
                        lostProbes++;
                }
                q = (double) lostProbes / probes.size();
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

    final private class ResponseTimes {
        private final Map<Long, ProbeRecord> probesSent;
        private final Map<Address, List<LatencyTime>> latencyTimes;
        private final List<Long> nextProbePeriods; // Milliseconds
        private final Map<Address, Integer> initialProbes;
        private boolean initialProbesReceived;
        private volatile double potentialProbePeriod; // Milliseconds
        private volatile double averageLatency; // Nanoseconds
        private volatile NMCData nmcData;

        public ResponseTimes() {
            latencyTimes = Collections.synchronizedMap(new HashMap<Address, List<LatencyTime>>());
            probesSent = Collections.synchronizedMap(new HashMap<Long, ProbeRecord>());
            nextProbePeriods = Collections.synchronizedList(new ArrayList<Long>());
            initialProbes = Collections.synchronizedMap(new HashMap<Address, Integer>());
            initialProbesReceived = false;
            potentialProbePeriod = -1;
            nmcData = new NMCData(0,0,0,0,0,0);
            averageLatency = 0.0;
        }

        // In milliseconds
        public void nextProbePeriod() {
            double ppp = potentialProbePeriod;
            int mpp = minProbeFrequency;

            synchronized (nextProbePeriods) {
                if (ppp > 0)
                    nextProbePeriods.add(ppp > mpp ? (long) Math.ceil(ppp) : mpp);
            }
        }

        public long getNextProbePeriod() {
            synchronized (nextProbePeriods) {
                if (nextProbePeriods.size() > 0)
                    return nextProbePeriods.get(nextProbePeriods.size() - 1);
                return 0;
            }
        }

        // TODO GARBAGE COLLECTION - implement a means for this method to be called
        public void removeOldPeriods() {
            int numberOfPeriods = (int) Math.ceil(1 / (1 - directLatencyProb));
            synchronized (nextProbePeriods) {
                if (nextProbePeriods.size() > numberOfPeriods) {
                    int periodsToRemove = nextProbePeriods.size() - numberOfPeriods;
                    Collection<Long> tmp = nextProbePeriods.subList(periodsToRemove, nextProbePeriods.size());
                    nextProbePeriods.clear();
                    nextProbePeriods.addAll(tmp);
                }
            }
        }

        public void initialProbesReceived() {
            initialProbesReceived = true;
            initialProbes.clear();
        }

        public void addInitialProbe(Address destination) {
            if (!initialProbesReceived) {
                synchronized (initialProbes) {
                    int newValue;
                    Integer currentValue = initialProbes.get(destination);
                    if (currentValue != null)
                        newValue = currentValue + 1;
                    else
                        newValue = 1;

                    initialProbes.put(destination, newValue);
                }
            }
        }

        public Map<Address, Integer> getInitialProbes() {
            return new HashMap<Address, Integer>(initialProbes);
        }

        public void addSentProbe(ProbeData probeData) {
            ProbeRecord record = new ProbeRecord(probeData.timeSent, false, false);
            probesSent.put(probeData.timeSent, record);
        }

        public void receiveProbe(ProbeData probeData) {
            // If returns null then the responding node was unknown at the time of broadcast
            // Ignore this latency
            if (probesSent.get(probeData.timeSent) != null) {
                addLatency(probeData);
                probesSent.get(probeData.timeSent).received = true;
                addInitialProbe(probeData.getAddress());
            }
        }

        public void discoverLostProbes() {
            synchronized (probesSent) {
                for (Long key : probesSent.keySet()) {
                    ProbeRecord probe = probesSent.get(key);
                    long nPP = getNextProbePeriod() * 2000000; // Convert npp to nanoseconds
                    long timeAlive = System.nanoTime() - probe.timeSent;

                    if (!probe.isLost && !probe.received && nPP > 0 && timeAlive > nPP)
                        probe.isLost = true;
                }
            }
        }

        // FRESHNESS_DURATION must be nanoseconds
        public void removeStaleProbes(long freshnessDuration) {
            synchronized (probesSent) {
                Iterator<Long> i = probesSent.keySet().iterator();
                while (i.hasNext()) {
                    Long timeSent = i.next();
                    if ((System.nanoTime() - timeSent) / 1000000 > freshnessDuration)
                        i.remove();
                }
            }
        }

        // FRESHNESS_DURATION must be nanoseconds
        public void removeStaleLatencies(long freshnessDuration) {
            synchronized (latencyTimes) {
                Iterator<Address> it = latencyTimes.keySet().iterator();
                while (it.hasNext())  {
                    Iterator<LatencyTime> i = latencyTimes.get(it.next()).iterator();
                    while (i.hasNext()) {
                        LatencyTime latency = i.next();
                        if ((System.nanoTime() - latency.freshness) / 1000000 > freshnessDuration)
                            i.remove();
                    }
                }
            }
        }

        public void addLatency(ProbeData probeData) {
            long probeLatency = System.nanoTime() - probeData.timeSent;
            Address probedNode = probeData.getAddress();
            LatencyTime latency = new LatencyTime(probeLatency, System.nanoTime());
            addLatency(probedNode, latency);
            receiveXMax(probeData);

            // If probe period has already been initialised update probe period with this latency time
            if (potentialProbePeriod > -1)
                potentialProbePeriod = (0.95 * potentialProbePeriod + 0.05 * (latency.latency / 2000000.0));
            averageLatency = (averageLatency + latency.latency) / 2.0;
        }

        // Extract xMax from probe and store as a latency
        public void receiveXMax(ProbeData probeData) {
            // Convert xMax into nanoseconds and multiply by two as latencies are stored as the round trip time
            if (probeData.xMax > 0) {
                int xMaxLatency = probeData.xMax * 2000000;
                addLatency(probeData.getAddress(), new LatencyTime(xMaxLatency, System.nanoTime()));
            }
        }

        public void addLatency(Address probedNode, LatencyTime latency) {
            synchronized (latencyTimes) {
                if (!latencyTimes.containsKey(probedNode)) {
                    List<LatencyTime> latencies = new ArrayList<LatencyTime>();
                    latencies.add(latency);
                    latencyTimes.put(probedNode, latencies);
                }
                else
                    latencyTimes.get(probedNode).add(latency);
            }
        }

        public Map<Address, List<LatencyTime>> getAllTimes() {
            return new HashMap<Address, List<LatencyTime>>(latencyTimes);
        }

        public List<ProbeRecord> getProbeRecords() {
            return new ArrayList<ProbeRecord>(probesSent.values());
        }

        public List<Long> getNextProbePeriods() {
            return new ArrayList<Long>(nextProbePeriods);
        }
    }

    protected static class ProbeHeader extends Header {
        public static final byte PROBE_REQ=1;
        public static final byte PROBE_RSP=2;

        private byte type = 0;
        private ProbeData data = null;
        private String clusterName = null;

        public ProbeHeader() {
        }

        public ProbeHeader(byte type) {
            this.type = type;
        }

        public ProbeHeader(byte type, ProbeData data) {
            this(type);
            this.data = data;
        }

        public ProbeHeader(byte type, ProbeData data, String clusterName) {
            this(type, data);
            this.clusterName = clusterName;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[Probe: type=" + typeToString(type));
            if(data != null)
                sb.append(", arg=" + data);
            if(clusterName != null)
                sb.append(", cluster=").append(clusterName);
            sb.append(']');
            return sb.toString();
        }

        String typeToString(byte type) {
            switch(type) {
                case PROBE_REQ: return "PROBE_REQ";
                case PROBE_RSP: return "PROBE_RSP";
                default:        return "<unknown type(" + type + ")>";
            }
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
            Bits.writeString(clusterName, out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            type = in.readByte();
            data = (ProbeData) Util.readStreamable(ProbeData.class, in);
            clusterName = Bits.readString(in);
        }
    }

    protected static class ProbeData extends PingData {
        protected int xMax = 0; // xMax - The largest latency encountered by the sending node
        protected long timeSent = -1L; // The System.nanoTime() at which the probe was created

        public ProbeData() {
        }

        public ProbeData(Address sender, View view, String logicalName,
                         Collection<PhysicalAddress> physicalAddresses, int xMax, long timeSent) {
            super(sender, view, false, logicalName, physicalAddresses);
            this.xMax = xMax;
            this.timeSent = timeSent;
        }

        @Override
        public String toString() {
            return super.toString() + ", xMax=" + xMax + ", timeSent=" + timeSent;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            super.writeTo(out);
            out.writeInt(xMax);
            out.writeLong(timeSent);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            super.readFrom(in);
            xMax = in.readInt();
            timeSent = in.readLong();
        }

        @Override
        public int size() {
            return super.size() + Global.INT_SIZE + Bits.size(timeSent);
        }
    }

    private class ProbeRecord {
        final long timeSent; // Nano Time
        volatile boolean isLost;
        volatile boolean received;

        public ProbeRecord(long timeSent, boolean isLost, boolean received) {
            this.timeSent = timeSent;
            this.isLost = isLost;
            this.received = received;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProbeRecord that = (ProbeRecord) o;
            if (timeSent != that.timeSent) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return (int) (timeSent ^ (timeSent >>> 32));
        }
    }

    private class LatencyTime implements Comparable<LatencyTime> {
        final long latency; // Nanoseconds
        final long freshness; // Nanoseconds

        public LatencyTime(long latency, long freshness) {
            this.latency = latency;
            this.freshness = freshness;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LatencyTime that = (LatencyTime) o;
            if (freshness != that.freshness) return false;
            if (latency != that.latency) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (latency ^ (latency >>> 32));
            result = 31 * result + (int) (freshness ^ (freshness >>> 32));
            return result;
        }

        @Override
        public int compareTo(LatencyTime other) {
            if (this.equals(other))
                return 0;
            else if(latency > other.latency)
                return 1;
            else
                return -1;
        }
    }
}