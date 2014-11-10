package org.jgroups.protocols.aramis;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that represents the Network Measurement Component (NMC) of Aramis.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class LongNMC implements NMC {

    private final double ETA_PROBABILITY = 0.99;
    private final double RELIABILITY_PROBABILITY = 0.9999;
    private final double MAX_Q_VALUE = 0.05;
    private final double Q_MULTIPLIER = 1 + MAX_Q_VALUE; // The threshold for calculating Q
    private final int XRC_SAMPLE_SIZE = 10; // The minimum number of values we use to calculate R
    private final int RECENT_PAST_SIZE = 1000; // The number of latencies that defines the recent past
    private final int EPOCH_SIZE = 100; // The number of latencies received before NMC values are calculated

    private final PCSynch clock;
    private final Profiler profiler;
    private final List<Long> currentLatencies = new ArrayList<Long>(EPOCH_SIZE);
    private final List<Long> recentPastLatencies = Collections.synchronizedList(new ArrayList<Long>());
    private final ReentrantLock lock = new ReentrantLock(false);
    private NMCData nmcData;
    private volatile int activeNodes = 0;
    private volatile long xMax = 0;

    private Log log = LogFactory.getLog(Aramis.class);

    private final NMCProfiler nmcProfiler = new NMCProfiler(); // TODO remove
    private final Aramis rmsys;

    public LongNMC(PCSynch clock, Aramis aramis, final Profiler profiler) {
        this.clock = clock;
        this.profiler = profiler;
        this.rmsys = aramis;

        profiler.longNMCUsed(); // Probably a better way to do this.

        // TODO remove
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Profiler ------- \n " + profiler);
                System.out.println("NMC  -------\n" + nmcProfiler);
//                try {
//                    nmcProfiler.collectionToFile(nmcProfiler.localXMax, "local");
//                } catch (Exception e) {
//                    System.out.println("WriteToFile Exception | " + e);
//                }
            }
        }));
    }

    public NMCData getData() {
        return nmcData;
    }

    public void setActiveNodes(int numberOfNodes) {
        activeNodes = numberOfNodes;
    }

    public boolean initialProbesReceived() {
        int rplSize = recentPastLatencies.size();
        return rplSize > 0 && rplSize % EPOCH_SIZE == 0;
    }

    public void receiveProbe(RMCastHeader header) {
        long timeReceived = clock.getTime();
        long duration = timeReceived - header.getId().getTimestamp();
        long probeLatency = duration + clock.getMaximumError();

        if (log.isTraceEnabled())
            log.trace("Probe latency of " + TimeUnit.NANOSECONDS.toMillis(probeLatency) + "ms recorded for " + header);

        lock.lock();
        try {
            addLatency(probeLatency);
        } finally {
            lock.unlock();
        }
    }

    // Return XMax / xMax
    public double calculateR() throws Exception {
        ExceedsXrcResult exceeds = getLatenciesThatExceedXrc();

        // Throw an exception if R can't be calculated
        if (exceeds.latencies.isEmpty())
            throw new Exception("No messages exceed the Xrc");

        int total = 0;
        long xMax = exceeds.xMax;
        for (Long i : exceeds.latencies)
            total += i - xMax;

        double divisor = Math.max(XRC_SAMPLE_SIZE, exceeds.latencies.size());
        double marginalPeakAverage = total / divisor;

        return (xMax + marginalPeakAverage) / xMax;
    }

    private ExceedsXrcResult getLatenciesThatExceedXrc() {
        ExceedsXrcResult result;
        List<Long> cl;
        lock.lock();
        try {
            result = new ExceedsXrcResult(xMax);
            cl = new ArrayList<Long>(currentLatencies);
        } finally {
            lock.unlock();
        }

        List<Long> latencies = result.latencies;
        long threshold = result.xMax;
        for (Long latency : cl)
            if (latency > threshold)
                latencies.add(latency);

        return result;
    }

    private void addXMax(long maxLatency) {
        xMax = maxLatency; // Don't use a 'buffer' function
        profiler.addLocalXmax(xMax); // Store local xMax
        nmcProfiler.addXMax(xMax);
    }

    private void addLatency(long latency) {
        if (currentLatencies.size() < EPOCH_SIZE)
            currentLatencies.add(latency);

        if (currentLatencies.size() % EPOCH_SIZE == 0) {
            addCurrentLatenciesToRecentPast();
            calculateNMCValues();
        }
        profiler.addProbeLatency(latency);
    }

    private void addCurrentLatenciesToRecentPast() {
        int rplSize = recentPastLatencies.size();
        if (rplSize == RECENT_PAST_SIZE) {
            int startIndex = rplSize - EPOCH_SIZE;
            recentPastLatencies.subList(startIndex, rplSize).clear(); // Remove outdated latencies
        }
        recentPastLatencies.addAll(0, currentLatencies);
        currentLatencies.clear();
    }

    private void calculateNMCValues() {
        List<Long> latencies = new ArrayList<Long>(recentPastLatencies);
        Collections.sort(latencies);

        int numberOfLatencies = latencies.size();
        long maxLatency = latencies.get(numberOfLatencies - 1); // Ordered ascending, so last index is the largest latency
        long d = -1;
        int exceedQThreshold = 0;
        int count = 1;

        for (long i : latencies) {
            if (count >= numberOfLatencies * 0.693 && d < 0)
                d = i;

            if (i * Q_MULTIPLIER > maxLatency)
                exceedQThreshold++;

            count++;
        }
        addXMax(maxLatency);

        double q = calculateQ(exceedQThreshold, numberOfLatencies);
        int rho = calculateRho(q);
        long eta = (long) Math.ceil(-1 * d * Math.log(1 - ETA_PROBABILITY)); // Calculate 1 - e - Np / d = 0.99
        long omega = eta - d;
        long capD = xMax + (rho * eta);
        long capS = xMax + (2 * eta) + omega; // 2 * eta includes the max possible random wait used by a disseminating node

        nmcData = new NMCData(eta, 1, omega, capD, capS, xMax, clock.getTime()); // Create a timestamped NMCData

        if (log.isDebugEnabled())
            log.debug("NMCData recorded | " + nmcData);

        nmcProfiler.addEta(eta);
        nmcProfiler.addOmega(omega);
        nmcProfiler.addRho(rho);
        nmcProfiler.addQ(q);
    }

    private double calculateQ(int exceedMaxLatency, int numberOfLatencies) {
        double q = exceedMaxLatency / (double) numberOfLatencies;

        if (q > MAX_Q_VALUE) {
            double powerOfValue = ((1.0 / activeNodes) - 1.0);
            q = 1 - Math.pow(RELIABILITY_PROBABILITY, powerOfValue);
        }
        return q;
    }

    private int calculateRho(double q) {
        if (q == 1)
            throw new IllegalArgumentException("CalculateRho(double q), q cannot be equal to 1 as it results in an infinite loop!" +
            "Check yo self before you wreck yo self!");

        int rho = 0;
        double rhoProbability = 0.0;
        while (rhoProbability < RELIABILITY_PROBABILITY && rhoProbability <= 1.0) {
            rho++; // Ensures that rho is always > 0 as it will always be executed at least once.
            rhoProbability = Math.pow(1.0 - Math.pow(q, rho + 1), activeNodes - 1);
        }
//        return rho < 2 ? 2 : rho; // Rho artificially set to a minimum value of 2
        return rho; // Uncomment to use calculated rho that can have the minimum rho value for reliable multicast (rho = 1)
    }

    private class ExceedsXrcResult {
        private long xMax;
        private List<Long> latencies;

        ExceedsXrcResult(long xMax) {
            this.xMax = xMax;
            this.latencies = new ArrayList<Long>();
        }

        @Override
        public String toString() {
            return "ExceedsXrcResult{" +
                    "xMax=" + xMax +
                    ", latencies=" + latencies +
                    '}';
        }
    }

    private class NMCProfiler {
        final ArrayList<Long> localXMax = new ArrayList<Long>();
        final ArrayList<Long> localOmega = new ArrayList<Long>();
        final ArrayList<Long> localEta = new ArrayList<Long>();
        final ArrayList<Integer> localRho = new ArrayList<Integer>();
        final ArrayList<Double> localQ = new ArrayList<Double>();

        PrintWriter out;

        void addXMax(long xMax) {
            localXMax.add(xMax);
        }

        void addEta(long eta) {
            localEta.add(eta);
        }

        void addOmega(long omega) {
            localOmega.add(omega);
        }

        void addRho(int rho) {
            localRho.add(rho);
        }

        void addQ(double q) {
            localQ.add(q);
        }

        double averageDouble(Collection<Double> collection) {
            double total = 0;
            for (double d : collection)
                total += d;

            return total / (double) collection.size();
        }

        double averageInt(Collection<Integer> collection) {
            int total = 0;
            for (int i : collection)
                total += i;

            return total / (double) collection.size();
        }

        double average(Collection<Long> collection) {
            int total = 0;
            for (long i : collection)
                total += i;

            return total / (double) collection.size();
        }

        long median(List<Long> list) {
            Collections.sort(list);
            return list.get((int) Math.round(list.size() / (double) 2));
        }

        double medianDouble(List<Double> list) {
            Collections.sort(list);
            return list.get((int) Math.round(list.size() / (double) 2));
        }

        int medianInt(List<Integer> list) {
            Collections.sort(list);
            return list.get((int) Math.round(list.size() / (double) 2));
        }

        String restrictedOutput(Collection<Long> collection, boolean showCount) {
            int count = 1;
            long previous = -1;
            String output = "";
            for (Long i : collection) {
                if (i == previous || previous == -1) {
                    count++;
                } else {
                    if (showCount)
                        output += previous + "(" + count + ")\n";
                    else
                        output += previous + "\n";
                    count = 1;
                }
                previous = i;
            }
            return output;
        }

        String output(Collection<Long> collection) {
            String output = "";
            for (Long i : collection)
                output += i + "\n";
            return output;
        }

        void collectionToFile(Collection<Long> collection, String name, int type) throws Exception {
            String PATH = "/work/a7109534/";
            Address localAddress = rmsys.getLocalAddress();

            String output = "";
            String filename = "";
            switch (type) {
                case 0:
                    output = output(collection);
                    filename = "xMax";
                    break;
                case 1:
                    output = restrictedOutput(collection, false);
                    filename = "xMaxRestricted";
                    break;
                case 2:
                    output = restrictedOutput(collection, true);
                    filename = "xMaxCount";
                    break;
            }

            String filePath = PATH + filename + "-" + name + "-" + localAddress + ".csv";
            out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)), true);
            out.print(output);
            out.flush();
            out.close();
        }

        void collectionToFile(Collection<Long> collection, String name) throws Exception {
            collectionToFile(collection, name, 1);
            collectionToFile(collection, name, 2);
            collectionToFile(collection, name, 0);
        }

        @Override
        public String toString() {
            return "NMCProfiler{" +
                    "\n\tlocalXMax{" +
                    "\n\t\tLargest := " + convertToMilli(Collections.max(localXMax)) +
                    "\n\t\tSmallest := " + convertToMilli(Collections.min(localXMax)) +
                    "\n\t\tMedian := " + convertToMilli(median(localXMax)) +
                    "\n\t\tAverage := " + Math.round(average(localXMax) / 1e+6) / 100.0 +
                    "}, " +
                    "\n\tOmega{" +
                    "\n\t\tLargest := " + convertToMilli(Collections.max(localOmega)) +
                    "\n\t\tSmallest := " + convertToMilli(Collections.min(localOmega)) +
                    "\n\t\tMedian := " + convertToMilli(median(localOmega)) +
                    "\n\t\tAverage := " +  Math.round(average(localOmega) / 1e+6) / 100.0 +
                    "}, " +
                    "\n\tEta{" +
                    "\n\t\tLargest := " + convertToMilli(Collections.max(localEta)) +
                    "\n\t\tSmallest := " + convertToMilli(Collections.min(localEta)) +
                    "\n\t\tMedian := " + convertToMilli(median(localEta)) +
                    "\n\t\tAverage := " +  Math.round(average(localEta) / 1e+6) / 100.0 +
                    "}, " +
                    "\n\tRho{" +
                    "\n\t\tLargest := " + Collections.max(localRho) +
                    "\n\t\tSmallest := " + Collections.min(localRho) +
                    "\n\t\tMedian := " + medianInt(localRho) +
                    "\n\t\tAverage := " +  Math.round(averageInt(localRho) * 100.0) / 100.0 +
                    "}, " +
                    "\n\tQ{" +
                    "\n\t\tLargest := " + Collections.max(localQ) +
                    "\n\t\tSmallest := " + Collections.min(localQ) +
                    "\n\t\tMedian := " + medianDouble(localQ) +
                    "\n\t\tAverage := " + averageDouble(localQ) +
                    "}, " +
                    '}';
        }

        private double convertToMilli(long value) {
            double valueInMilli = value / 1e+6;
            return Math.round(valueInMilli * 100.0) / 100.0;
        }
    }
}