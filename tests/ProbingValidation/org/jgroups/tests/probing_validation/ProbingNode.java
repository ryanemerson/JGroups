package org.jgroups.tests.probing_validation;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * A class executed by a probing node.
 *
 * @author a7109534
 * @since 4.0
 */
public class ProbingNode  extends ReceiverAdapter {
    private final List<LatencyTime> pastLatencies = Collections.synchronizedList(new ArrayList<LatencyTime>());
    private final List<LatencyTime> presentLatencies = Collections.synchronizedList(new ArrayList<LatencyTime>());
    private final short MASTER_HEADER_ID;
    private final short PROBING_HEADER_ID;
    private final String PROPERTIES_FILE;
    private final int NUMBER_OF_PROBING_NODES;
    private final int NUMBER_OF_ROUNDS;
    private final int ROUND_DURATION;
    private volatile Address masterNode;
    private JChannel channel;


    public ProbingNode(short masterHeaderId, short probingHeaderId, String propertiesFile, int numberOfProbingNodes, int numberOfRounds, int roundDuration) {
        this.MASTER_HEADER_ID = masterHeaderId;
        this.PROBING_HEADER_ID = probingHeaderId;
        this.PROPERTIES_FILE = propertiesFile;
        this.NUMBER_OF_PROBING_NODES = numberOfProbingNodes;
        this.NUMBER_OF_ROUNDS = numberOfRounds;
        this.ROUND_DURATION = roundDuration;

        System.out.println("I am a slave node");
    }

    public void run() throws Exception {
        channel = new JChannel(PROPERTIES_FILE);
        channel.setReceiver(this);
        channel.connect("validation");

        ClassConfigurator.add(PROBING_HEADER_ID, ProbingHeader.class); // Add Header to magic map without adding to file
        ClassConfigurator.add(MASTER_HEADER_ID, MasterHeader.class); // Add Header to magic map without adding to file

        while (true) {}
    }

    public void viewAccepted(View view) {
        System.out.println("New View := " + view);
    }

    public void receive(Message msg) {
        synchronized (this) {
            MasterHeader masterHeader = (MasterHeader) msg.getHeader(MASTER_HEADER_ID);
            if (masterHeader != null) {
                System.out.println("Receive Master Request | " + masterHeader);
                handleMasterRequest(msg, masterHeader);
                return;
            }

            ProbingHeader probe = (ProbingHeader) msg.getHeader(PROBING_HEADER_ID);
            if (probe != null) {
                System.out.println("Receive Probe | Src := " + msg.getSrc() + " | " + probe);
                Address localAddress = channel.getAddress();

                if (probe.getOriginator().equals(localAddress)) {
                    List<LatencyTime> latencies = probe.isPresent() ? presentLatencies : pastLatencies;
                    latencies.add(new LatencyTime(localAddress, (System.nanoTime() - probe.getTimeSent())));

                    System.out.println("Response received from " + msg.getSrc() + " | probes received := " + latencies.size());

                    if (allProbesReceived(latencies)) {
                        System.out.println("All probes received! | past := " + !probe.isPresent());
                        byte type = probe.isPresent() ? MasterHeader.PRESENT_LATENCIES : MasterHeader.PAST_LATENCIES;
                        MasterHeader header = MasterHeader.probingComplete(type, probe.getTimePeriod(), latencies);
                        sendResponseToMaster(header);
                    }
                } else {
                    sendResponseToProbingNode(msg, probe);
                }
            }
        }
    }

    private void handleMasterRequest(Message message, MasterHeader header) {
        System.out.println("Receive Master Request");

        masterNode = message.getSrc();
        pastLatencies.clear();
        presentLatencies.clear();

        Executors.newSingleThreadExecutor().execute(new ProbeMulticaster(header));
    }

    private boolean allProbesReceived(List<LatencyTime> latencies) {
        return latencies.size() == NUMBER_OF_ROUNDS * (NUMBER_OF_PROBING_NODES - 1);
    }

    private void sendResponseToProbingNode(Message message, ProbingHeader probe) {
        sendMessage(new Message(message.getSrc(), message.getBuffer()).putHeader(PROBING_HEADER_ID, probe));
        System.out.println("Send probe back to " + probe.getOriginator() + " | " + message);
    }

    private void sendResponseToMaster(MasterHeader header) {
        System.out.println("Send response to master | tp := " + header.getTimePeriod() + " | period := " + MasterHeader.type2String(header.getType()));
        sendMessage(createMessageAddHeader(MASTER_HEADER_ID, header).dest(masterNode));
    }

    private Message createMessageAddHeader(short id, Header header) {
        return new Message().setBuffer(new byte[2000]).putHeader(id, header);
    }

    private void sendMessage(Message message) {
        try {
            channel.send(message);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    class ProbeMulticaster implements Runnable {
        final MasterHeader request;
        ProbeMulticaster(MasterHeader request) {
            this.request = request;
        }

        @Override
        public void run() {
            int round = 0;
            while (round < NUMBER_OF_ROUNDS * 2) {
                System.out.println("Start probing | Round := " + round);

                byte probeType = round == 1 || round <= (NUMBER_OF_ROUNDS - 1) ? ProbingHeader.PROBING_PAST : ProbingHeader.PROBING_PRESENT;
                broadcastProbe(new ProbingHeader(probeType, channel.getAddress(), request.getTimePeriod(), System.nanoTime()));

                Util.sleep(ROUND_DURATION);
                round++;
            }
        }

        private void broadcastProbe(ProbingHeader header) {
            System.out.println("--- Broadcast Probe | " + header);
            Message message = createMessageAddHeader(PROBING_HEADER_ID, header);
            for (Address address : request.getDestinations()) {
                if (!address.equals(channel.getAddress()))
                    sendMessage(message.copy().dest(address));
            }
        }
    }
}