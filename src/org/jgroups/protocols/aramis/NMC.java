package org.jgroups.protocols.aramis;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that represents the Network Measurement Component (NMC) of Aramis.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class NMC {

    private double reliabilityProb = 0.9999;
    private int epochSize = 100; // The number of latencies received before NMC values are calculated
    private int recentPastSize = 1000; // The number of latencies that defines the recent past
    private double qThreshold = .95; // The threshold for calculating Q
    private double etaProbability = 0.99;
    private double alpha = 0.9; // The value alpha that is used to update xMax
    private int xrcSampleSize = 10; // The minimum number of values we use to calculate R

    private final PCSynch clock;
    private final Profiler profiler;
    private final List<Integer> currentLatencies = new ArrayList<Integer>(epochSize);
    private final List<Integer> recentPastLatencies = Collections.synchronizedList(new ArrayList<Integer>());
    private final ReentrantLock lock = new ReentrantLock(false);
    private NMCData nmcData;
    private volatile int activeNodes = 0;
    private volatile int xMax = 0;

    private Log log = LogFactory.getLog(Aramis.class);

    private final NMCProfiler nmcProfiler = new NMCProfiler(); // TODO remove
    private final Aramis rmsys;

    public NMC(PCSynch clock, Aramis aramis, final Profiler profiler) {
        this.clock = clock;
        this.profiler = profiler;
        this.rmsys = aramis;

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

        int xMax = exceeds.nmcData.getXMax();
        int total = 0;
        for (Integer i : exceeds.latencies)
            total += i - xMax;

        double divisor = Math.max(xrcSampleSize, exceeds.latencies.size());
        double marginalPeakAverage = total / divisor;

        return (xMax + marginalPeakAverage) / xMax;
    }

    private ExceedsXrcResult getLatenciesThatExceedXrc() {
        ExceedsXrcResult result;
        List<Integer> cl;
        lock.lock();
        try {
            result = new ExceedsXrcResult(nmcData);
            cl = new ArrayList<Integer>(currentLatencies);
        } finally {
            lock.unlock();
        }

        List<Integer> latencies = result.latencies;
//        double threshold = result.nmcData.getXMax() + (result.nmcData.getEta() / 2); // Original Xrc
        double threshold = result.nmcData.getXMax();
        for (Integer latency : cl)
            if (latency > threshold)
                latencies.add(latency);

        return result;
    }

    private void addXMax(int maxLatency) {
        xMax = maxLatency; // Don't use a 'buffer' function
//        xMax = (int) Math.ceil(((1 - alpha) * xMax) + (alpha * maxLatency));
        profiler.addLocalXmax(xMax); // Store local xMax

        nmcProfiler.localXMax.add(xMax);
    }

    private void addLatency(int latency) {
        if (currentLatencies.size() < epochSize)
            currentLatencies.add(latency);

        if (currentLatencies.size() % epochSize == 0) {
            addCurrentLatenciesToRecentPast();
            calculateNMCValues();
        }
        profiler.addProbeLatency(latency);
    }

    private void addCurrentLatenciesToRecentPast() {
        int rplSize = recentPastLatencies.size();
        if (rplSize == recentPastSize) {
            int startIndex = rplSize - epochSize;
            recentPastLatencies.subList(startIndex, rplSize).clear(); // Remove outdated latencies
        }
        recentPastLatencies.addAll(0, currentLatencies);
        currentLatencies.clear();
    }

    private void calculateNMCValues() {
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
        int processedLatencies = 0; // Number of latencies that have been processed thus far
        int exceedQThreshold = 0;
        boolean dFlag = false;

        List<Integer> keys = new ArrayList<Integer>(latencyMap.keySet());
        Collections.sort(keys); // Ensure that keys are iterated in ascending order
        CLOOP:
        for (Integer key : keys) {
            int[] tempLatencies = latencyMap.get(key);
            for (int yy = 0; yy < tempLatencies.length; yy++) {
                int latency = key + yy;
                if (latency > maxLatency)
                    break CLOOP;

                processedLatencies += tempLatencies[yy];
                if (processedLatencies >= numberOfLatencies * 0.693 && !dFlag) {
                    d = latency;
                    dFlag = true;
                }

//                if (tempLatencies[yy] > 0 && latency > maxLatency * qThreshold)
                if (tempLatencies[yy] > 0 && latency * 1.05 > maxLatency)
                    exceedQThreshold += tempLatencies[yy];
            }
        }
        addXMax(maxLatency);

        double q = exceedQThreshold / (double) numberOfLatencies;
        int rho = calculateRho(q);
        int eta = (int) Math.ceil(-1 * d * Math.log(1 - etaProbability)); // Calculate 1 - e - Np / d = 0.99
        int omega = eta - d;
        int capD = xMax + (rho * eta);
        int capS = xMax + (2 * eta) + omega; // 2 * eta includes the max possible random wait used by a disseminating node

        // MessageCopies hard coded to 1 to reduce total message copies, true rho value still used for calculating delivery delays
        nmcData = new NMCData(eta, 1, omega, capD, capS, xMax, clock.getTime()); // Create a timestamped NMCData
//        System.out.println(nmcData + " | rho := " + rho + " | q := " + q);

//        if (rho > 10) {
//            List<Integer> tempList = new ArrayList<Integer>(recentPastLatencies);
//            Collections.sort(tempList);
//            System.out.println("Q := " + q + " | rho := " + rho + " | exceedQThreshold := " + exceedQThreshold + " | numberOfLatencies := " + numberOfLatencies +
//                    " | deliveryDelay := " + TimeUnit.NANOSECONDS.toMillis(calculateDeliveryTime(nmcData)) + " | " + tempList + "\n" + nmcData);
//        }

        if (log.isDebugEnabled())
            log.debug("NMCData recorded | " + nmcData);

        nmcProfiler.add(nmcData);
        nmcProfiler.addRho(rho);
        nmcProfiler.addQ(q);
    }

    // TODO remove
    private long calculateDeliveryTime(NMCData data) {
        long delay = data.getCapS() + data.getCapD();
        long ackWait = (2 * data.getEta()) + data.getOmega();
        delay = (2 * delay) + ackWait;

        return TimeUnit.MILLISECONDS.toNanos(delay) + (2 * 1000000); // Convert to Nanos and add epislon
    }

    // Forces decimals to always round up, pessimistic!
    private int convertToMilli(long value) {
        if (value < 1000000)
            return 1;

        long result = value / 1000000;
        if (result > Integer.MAX_VALUE)
            throw new IllegalArgumentException("The calculated long value is greater than Iteger.MAX_VALUE | " +
                    "input := " + value + " | calculated value := " + result);
        return (int) Math.ceil(value / 1000000d);
    }

    private int calculateRho(double q) {
        int rho = 0;
        double rhoProbability = 0.0;
        while (q != 1 && rhoProbability < reliabilityProb && rhoProbability <= 1.0) {
            rho++; // Ensures that rho is always > 0 as it will always be executed at least once.
            rhoProbability = Math.pow(1.0 - Math.pow(q, rho + 1), activeNodes - 1);
        }
//        return rho < 2 ? 2 : rho; // Rho artificially set to a minimum value of 2
        return rho; // Uncomment to use calculated rho that can have the minimum rho value for reliable multicast (rho = 1)
    }

    private class ExceedsXrcResult {
        private NMCData nmcData;
        private List<Integer> latencies;

        ExceedsXrcResult(NMCData nmcData) {
            this.nmcData = nmcData;
            this.latencies = new ArrayList<Integer>();
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
        final ArrayList<Integer> localXMax = new ArrayList<Integer>();
        final ArrayList<Integer> localOmega = new ArrayList<Integer>();
        final ArrayList<Integer> localEta = new ArrayList<Integer>();
        final ArrayList<Integer> localRho = new ArrayList<Integer>();
        final ArrayList<Double> localQ = new ArrayList<Double>();

        PrintWriter out;

        void add(NMCData data) {
            localOmega.add(data.getOmega());
            localEta.add(data.getEta());
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

        double average(Collection<Integer> collection) {
            int total = 0;
            for (int i : collection)
                total += i;

            return total / (double) collection.size();
        }

        int median(List<Integer> list) {
            Collections.sort(list);
            return list.get((int) Math.round(list.size() / (double) 2));
        }

        double medianDouble(List<Double> list) {
            Collections.sort(list);
            return list.get((int) Math.round(list.size() / (double) 2));
        }

        String restrictedOutput(Collection<Integer> collection, boolean showCount) {
            int previous = -1;
            int count = 1;
            String output = "";
            for (Integer i : collection) {
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

        String output(Collection<Integer> collection) {
            String output = "";
            for (Integer i : collection)
                output += i + "\n";
            return output;
        }

        void collectionToFile(Collection<Integer> collection, String name, int type) throws Exception {
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

        void collectionToFile(Collection<Integer> collection, String name) throws Exception {
            collectionToFile(collection, name, 1);
            collectionToFile(collection, name, 2);
            collectionToFile(collection, name, 0);
        }

        @Override
        public String toString() {
            return "NMCProfiler{" +
                    "\n\tlocalXMax{" +
                    "\n\t\tLargest := " + Collections.max(localXMax) +
                    "\n\t\tSmallest := " + Collections.min(localXMax) +
                    "\n\t\tMedian := " + median(localXMax) +
                    "\n\t\tAverage := " + average(localXMax) +
                    "}, " +
                    "\n\tOmega{" +
                    "\n\t\tLargest := " + Collections.max(localOmega) +
                    "\n\t\tSmallest := " + Collections.min(localOmega) +
                    "\n\t\tMedian := " + median(localOmega) +
                    "\n\t\tAverage := " + average(localOmega) +
                    "}, " +
                    "\n\tEta{" +
                    "\n\t\tLargest := " + Collections.max(localEta) +
                    "\n\t\tSmallest := " + Collections.min(localEta) +
                    "\n\t\tMedian := " + median(localEta) +
                    "\n\t\tAverage := " + average(localEta) +
                    "}, " +
                    "\n\tRho{" +
                    "\n\t\tLargest := " + Collections.max(localRho) +
                    "\n\t\tSmallest := " + Collections.min(localRho) +
                    "\n\t\tMedian := " + median(localRho) +
                    "\n\t\tAverage := " + average(localRho) +
                    "}, " +
                    "\n\tQ{" +
                    "\n\t\tLargest := " + Collections.max(localQ) +
                    "\n\t\tSmallest := " + Collections.min(localQ) +
                    "\n\t\tMedian := " + medianDouble(localQ) +
                    "\n\t\tAverage := " + averageDouble(localQ) +
                    "}, " +
                    '}';
        }
    }
}