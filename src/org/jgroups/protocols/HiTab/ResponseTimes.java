package org.jgroups.protocols.HiTab;

import org.jgroups.Address;

import java.util.*;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
final public class ResponseTimes {
    private final Map<Long, ProbeRecord> probesSent;
    private final Map<Address, List<LatencyTime>> latencyTimes;
    private final List<Long> nextProbePeriods; // Milliseconds
    private final Map<Address, Integer> initialProbes;
    private boolean initialProbesReceived;
    private double directLatencyProbability; // e.g 0.9999
    private volatile double potentialProbePeriod; // Milliseconds
    private volatile double averageLatency; // Nanoseconds
    private volatile int minimumProbePeriod; // Milliseconds
    private volatile NMCData nmcData;

    public ResponseTimes(int minimumProbePeriod, double directLatencyProbability) {
        this.minimumProbePeriod = minimumProbePeriod;
        this.directLatencyProbability = directLatencyProbability;

        latencyTimes = Collections.synchronizedMap(new HashMap<Address, List<LatencyTime>>());
        probesSent = Collections.synchronizedMap(new HashMap<Long, ProbeRecord>());
        nextProbePeriods = Collections.synchronizedList(new ArrayList<Long>());
        initialProbes = Collections.synchronizedMap(new HashMap<Address, Integer>());

        initialProbesReceived = false;
        potentialProbePeriod = -1;
        nmcData = new NMCData(0,0,0,0,0,0);
        averageLatency = 0.0;
    }

    public double getPotentialProbePeriod() { return potentialProbePeriod; }

    public void setPotentialProbePeriod(long nextProbePeriod) {
        potentialProbePeriod = nextProbePeriod;
    }

    // In milliseconds
    public void nextProbePeriod() {
        double ppp = potentialProbePeriod;
        int mpp = minimumProbePeriod;

        synchronized (nextProbePeriods) {
            if (ppp > 0) {
                nextProbePeriods.add(ppp > mpp ? (long) Math.ceil(ppp) : mpp);
            }
        }
    }

    public long getNextProbePeriod() {
        synchronized (nextProbePeriods) {
            if (nextProbePeriods.size() > 0) {
                return nextProbePeriods.get(nextProbePeriods.size() - 1);
            }
            return 0;
        }
    }

    // TODO GARBAGE COLLECTION - implement a means for this method to be called
    public void removeOldPeriods() {
        int numberOfPeriods = (int) Math.ceil(1 / (1 - directLatencyProbability));
        synchronized (nextProbePeriods) {
            if (nextProbePeriods.size() > numberOfPeriods) {
                int periodsToRemove = nextProbePeriods.size() - numberOfPeriods;
                Collection<Long> tmp = nextProbePeriods.subList(periodsToRemove, nextProbePeriods.size());
                nextProbePeriods.clear();
                nextProbePeriods.addAll(tmp);
            }
        }
    }

    public NMCData getNmcData() {
        return nmcData;
    }

    public void updateValues(NMCData newData) {
        nmcData = newData;
    }

    public double getAverageLatency() {
        return averageLatency;
    }

    public boolean isInitialProbesReceived() {
        return initialProbesReceived;
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

    public void setInitialProbes(Map<Address, Integer> probes) {
        initialProbes.putAll(probes);
    }

    public Map<Address, Integer> getInitialProbes() {
        return new HashMap<Address, Integer>(initialProbes);
    }

    public void addSentProbe(Address destination, ProbeData probeData) {
        ProbeRecord record = new ProbeRecord(destination, probeData.getTimeSent(), false, false);
        probesSent.put(probeData.getTimeSent(), record);
    }

    public void receiveProbe(ProbeData probeData) {
        // If returns null then the responding node was unknown at the time of broadcast
        // Ignore this latency
        if (probesSent.get(probeData.getTimeSent()) != null) {
            addLatency(probeData);
            probesSent.get(probeData.getTimeSent()).setReceived(true);
            addInitialProbe(probeData.getAddress());
        }
    }

    public void discoverLostProbes() {
        synchronized (probesSent) {
            for (Long key : probesSent.keySet()) {
                ProbeRecord probe = probesSent.get(key);

                long nPP = getNextProbePeriod() * 2000000; // Convert npp to nanoseconds
                long timeAlive = System.nanoTime() - probe.getTimeSent();

                if (!probe.isLost() && !probe.isReceived() && nPP > 0 && timeAlive > nPP) {
                    probe.setLost(true);
                }
            }
        }
    }

    // FRESHNESS_DURATION must be nanoseconds
    public void removeStaleProbes(long freshnessDuration) {
        synchronized (probesSent) {
            for (Long key : probesSent.keySet()) {
                ProbeRecord probe = probesSent.get(key);

                if ((System.nanoTime() - probe.getTimeSent()) / 1000000 > freshnessDuration) {
                    probesSent.remove(key);
                }
            }
        }
    }

    // FRESHNESS_DURATION must be nanoseconds
    public void removeStaleLatencies(long freshnessDuration) {
        synchronized (latencyTimes) {
            for (Address address : latencyTimes.keySet()) {
                for (LatencyTime latency : latencyTimes.get(address)) {
                    if ((System.nanoTime() - latency.getFreshness()) / 1000000 > freshnessDuration) {
                        latencyTimes.get(address).remove(latency);
                    }
                }
            }
        }
    }

    public void addLatency(ProbeData probeData) {
        long probeLatency = System.nanoTime() - probeData.getTimeSent();
        Address probedNode = probeData.getAddress();
        LatencyTime latency = new LatencyTime(probeLatency, System.nanoTime());
        addLatency(probedNode, latency);
        receiveXMax(probeData);

        // If probe period has already been initialised update probe period with this latency time
        if (potentialProbePeriod > -1) {
            potentialProbePeriod = (0.95 * potentialProbePeriod + 0.05 * (latency.getLatency() / 2000000.0));
        }
        averageLatency = (averageLatency + latency.getLatency()) / 2.0;
    }

    // Extract xMax from probe and store as a latency
    public void receiveXMax(ProbeData probeData) {
        // Convert xMax into nanoseconds and multiply by two as latencies are stored as the round trip time
        if (probeData.getXMax() > 0) {
            int xMaxLatency = probeData.getXMax() * 2000000;
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
            else {
                latencyTimes.get(probedNode).add(latency);
            }
        }
    }

    public Map<Address, List<LatencyTime>> getAllTimes() {
        return new HashMap<Address, List<LatencyTime>>(latencyTimes);
    }

    public Map<Long, ProbeRecord> getProbesSent() {
        return new HashMap<Long, ProbeRecord>(probesSent);
    }

    public List<ProbeRecord> getProbeRecords() {
        return new ArrayList<ProbeRecord>(probesSent.values());
    }

    public List<Long> getNextProbePeriods() {
        return new ArrayList<Long>(nextProbePeriods);
    }

    @Override
    public String toString() {
        return "ResponseTimes{" +
                "probesSent=" + probesSent +
                ", latencyTimes=" + latencyTimes +
                ", nextProbePeriods=" + nextProbePeriods +
                ", initialProbes=" + initialProbes +
                ", initialProbesReceived=" + initialProbesReceived +
                ", directLatencyProbability=" + directLatencyProbability +
                ", potentialProbePeriod=" + potentialProbePeriod +
                ", averageLatency=" + averageLatency +
                ", minimumProbePeriod=" + minimumProbePeriod +
                ", nmcData=" + nmcData +
                '}';
    }
}