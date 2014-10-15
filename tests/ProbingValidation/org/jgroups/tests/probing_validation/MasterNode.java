package org.jgroups.tests.probing_validation;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that represents the master node used in the validation experiments.
 *
 * @author a7109534
 * @since 4.0
 */
public class MasterNode extends ReceiverAdapter {

    private final short MASTER_HEADER_ID;
    private final AtomicInteger timePeriod = new AtomicInteger(1);
    private final String PROPERTIES_FILE;
    private final int NUMBER_OF_PROBING_NODES;
    private PrintStream resultsFile;
    private JChannel channel;

    public MasterNode(short masterHeaderId, String propertiesFile, int numberOfProbingNodes) {
        this.MASTER_HEADER_ID = masterHeaderId;
        this.PROPERTIES_FILE = propertiesFile;
        this.NUMBER_OF_PROBING_NODES = numberOfProbingNodes;

        try {
            resultsFile = new PrintStream(new FileOutputStream("/work/a7109534/results.txt"));
        } catch (IOException e) {
            System.out.println(e);
            System.exit(0);
        }

        System.out.println("I am the master node");
    }

    public void run() throws Exception {
        channel = new JChannel(PROPERTIES_FILE);
        channel.setReceiver(this);
        channel.connect("validation");

        ClassConfigurator.add(MASTER_HEADER_ID, MasterHeader.class); // Add Header to magic map without adding to file

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new LatencyRequest(), 1, 30, TimeUnit.MINUTES);
    }

    public void viewAccepted(View view) {
        System.out.println("New View := " + view);
    }

    public void receive(Message msg) {
        synchronized (this) {
            System.out.println("Latencies received from " + msg.getSrc());
            MasterHeader header = (MasterHeader) msg.getHeader(MASTER_HEADER_ID);

            if (header.getType() == MasterHeader.PAST_LATENCIES)
                System.out.println("Past latencies received | tp :=" + header.getTimePeriod());
            else
                System.out.println("Present latencies received | tp :=" + header.getTimePeriod());

            String identifier = header.getType() == MasterHeader.PAST_LATENCIES ? "G" : "P";
            for (LatencyTime latency : header.getLatencyTimes())
                printLatency(identifier, header.getTimePeriod(), latency);
        }
    }

    private void printLatency(String identifier, int timePeriod, LatencyTime latency) {
        resultsFile.println(identifier + timePeriod + "\t" + (latency.getLatency() / 2000000.0));
    }

    class LatencyRequest implements Runnable {
        Set<Address> usedDestinations = new HashSet<Address>();

        @Override
        public void run() {
            int requestsToSend = (int) Math.ceil((channel.getView().size() - 1) / NUMBER_OF_PROBING_NODES);
            System.out.println("Initiate Master Requests | #" + requestsToSend + " | view size := " + channel.getView().size() + " | numberNodes := " + NUMBER_OF_PROBING_NODES);
            try {
                for (int i = 0; i < requestsToSend; i++)
                    sendRequest(selectDestinations());
                usedDestinations.clear();
            } catch (IllegalArgumentException e) {
                System.out.println(e);
            } catch (Exception e) {
            }
        }

        private Collection<Address> selectDestinations() {
            Set<Address> destinations = new HashSet<Address>();
            List<Address> randomisedView = new ArrayList<Address>(channel.getView().getMembers());
            Collections.shuffle(randomisedView);
            for (Address address : randomisedView)
                if (!address.equals(channel.getAddress()) && !usedDestinations.contains(address) && destinations.size() < NUMBER_OF_PROBING_NODES) {
                    destinations.add(address);
                    usedDestinations.add(address);
                }

            if (destinations.size() < NUMBER_OF_PROBING_NODES)
                throw new IllegalArgumentException("Number of probing nodes must be > " + NUMBER_OF_PROBING_NODES +
                        " | currently " + destinations.size());

            return destinations;
        }

        private void sendRequest(Collection<Address> destinations) throws Exception {
            System.out.println("Sending probing request to := " + destinations);
            MasterHeader header = MasterHeader.startProbingMessage(timePeriod.getAndIncrement(), destinations);
            for (Address destination : destinations) {
                Message message = new Message(destination);
                message.putHeader(MASTER_HEADER_ID, header);
                channel.send(message);
            }
        }
    }
}