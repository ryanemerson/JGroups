package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that represents the Network Measurement Component (NMC) of RMSys
 *
 * @author ryan
 * @since 4.0
 */
public class NMC {

    private double reliabilityProb = 0.9999;
    private int epochSize = 250; // The number of latencies that correspond to a single latency
    private int recentPastSize = 1000; // The number of latencies that defines the recent past
    private double qThreshold = 1.05; // The threshold for calculating Q
    private double etaProbability = 0.90;

    private final PCSynch clock;
    private final List<Integer> currentLatencies = new ArrayList<Integer>(epochSize);
    private final List<Integer> recentPastLatencies = Collections.synchronizedList(new ArrayList<Integer>());
    private final ReentrantLock lock = new ReentrantLock(true);
    private NMCData nmcData;
    private volatile int activeNodes = 0;
    private volatile int xMax = 0;

    private Log log = LogFactory.getLog(RMSys.class);

    public NMC(PCSynch clock) {
        this.clock = clock;
    }

    public long getClockTime() {
        return clock.getTime();
    }

    public long getMaxClockError() {
        return clock.getMaximumError();
    }

    public NMCData getData() {
        return nmcData;
    }

    public void setActiveNodes(int numberOfNodes) {
        activeNodes = numberOfNodes;
    }

    public boolean initialProbesReceived() {
        int rplSize = recentPastLatencies.size();
        return rplSize > 0 && rplSize % epochSize == 0;
    }

    public void receiveProbe(RMCastHeader header) {
        long timeReceived = clock.getTime();
        long duration = timeReceived - header.getId().getTimestamp();
        long probeLatency = duration + clock.getMaximumError();
        int probeLatencyMilli = convertToMilli(probeLatency);

        if (log.isTraceEnabled())
            log.trace("Probe latency of " + probeLatencyMilli + "ms recorded for " + header);

        lock.lock();
        try {
            addLatency(probeLatencyMilli);
            NMCData data = header.getNmcData();
            if (data != null) // If data == null then there is no xMax to store (message must be an initial empty probe)
                addXMax(data.getXMax());
        } finally {
            lock.unlock();
        }
    }

    private void addXMax(int maxLatency) {
        if (maxLatency > xMax)
            xMax = maxLatency;
    }

    private void addLatency(int latency) {
        if (currentLatencies.size() < epochSize)
            currentLatencies.add(latency);

        log.trace("currentLatencies Size := " + currentLatencies.size());

        if (currentLatencies.size() == epochSize) {
            addCurrentLatenciesToRecentPast();
            calculateNMCValues();
        }
    }

    private void addCurrentLatenciesToRecentPast() {
        int rplSize = recentPastLatencies.size();
        if (rplSize == recentPastSize) {
            int startIndex = rplSize - epochSize;
            recentPastLatencies.subList(startIndex, rplSize - 1).clear(); // Remove outdated latencies
        }
        recentPastLatencies.addAll(0, currentLatencies);
        currentLatencies.clear();
    }

    private void calculateNMCValues() {
        if (!initialProbesReceived())
            return;

        if (log.isTraceEnabled())
            log.trace("Calculate NMC values");

        int maxLatency = 0; // Largest latency encountered
        int numberOfLatencies = recentPastLatencies.size();
        Map<Integer, int[]> latencyMap = new HashMap<Integer, int[]>();
        for (int latency : recentPastLatencies) {
            maxLatency = latency > maxLatency ? latency : maxLatency;

            int latencyKey = (int) Math.floor(latency / 100) * 100;
            int latencyValue = latency % 100; // Get last two digits of latency
            if (latencyMap.containsKey(latencyKey)) {
                latencyMap.get(latencyKey)[latencyValue]++;
            } else {
                int[] array = new int[100];
                array[latencyValue]++;
                latencyMap.put(latencyKey, array);
            }
        }
        int d = 0;
        int temp = 0;
        int exceedQThreshold = 0;
        boolean dFlag = false;

        List<Integer> keys = new ArrayList<Integer>(latencyMap.keySet());
        Collections.sort(keys); // Ensure that keys are iterated in ascending order
        CLOOP:
        for (Integer key : keys) {
            int[] tempLatencies = latencyMap.get(key);
            for (int yy = 0; yy < tempLatencies.length; yy++) {
                int latency = key + yy;
                if ((key + yy) > maxLatency)
                    break CLOOP;

                temp += tempLatencies[yy];
                if (temp >= numberOfLatencies * 0.64 && !dFlag) {
                    d = key + yy;
                    dFlag = true;
                }

                if (latency * qThreshold > maxLatency)
                    exceedQThreshold++;
            }
        }
        addXMax(maxLatency);

        double q = exceedQThreshold / numberOfLatencies;
        int rho = calculateRho(q);
        int eta = (int) Math.ceil(-1 * d * Math.log(1 - etaProbability)); // Calculate 1 - e - Np / d = 0.99
        int omega = (int) Math.ceil(eta - d);
        int capD = xMax + (rho * eta);
        int capS = xMax + ((rho + 2) * eta) + omega;
        nmcData = new NMCData(eta, rho, omega, capD, capS, xMax);

        if (log.isDebugEnabled())
            log.debug("NMCData recorded | " + nmcData);
    }

    // Forces decimals to always round up, pessimistic!
    private int convertToMilli(long value) {
        if (value < 1000000)
            return 1;

        long result = value / 1000000;
        if (result > Integer.MAX_VALUE)
            throw new IllegalArgumentException("The calculated long value is greater than Iteger.MAX_VALUE | " +
                    "input := " + value + " | calculated value := " +  result);
        return (int) Math.ceil(value / 1000000d);
    }

    // Calculated based upon this nodes notion of view size
    private int calculateRho(double q) {
        int numberOfNodes = activeNodes;

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