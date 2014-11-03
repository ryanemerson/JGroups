package org.jgroups.tests.ABService;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.aramis.RMCastHeader;
import org.jgroups.protocols.aramis.Aramis;
import org.jgroups.protocols.tom.ToaHeader;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test class to send abcasts between multiple nodes. This test assumes infinite clients in an AbaaS scenario (i.e
 * the ARP is always full).
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class InfiniteClients extends ReceiverAdapter {
    public static void main(String[] args) throws Exception{
        String propsFile = "RMSysIntegrated.xml";
        String initiator = "";
        int numberOfMessages = 100000; // #Msgs to be executed by this node
        int totalMessages = 1000000; // #Msgs to be sent by the whole cluster
        int minimumNumberOfNodes = -1;
        for (int i = 0; i < args.length; i++) {
            if("-config".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
            if("-t-messages".equals(args[i])) {
                totalMessages = Integer.parseInt(args[++i]);
                continue;
            }
            if("-nr-messages".equals(args[i])) {
                numberOfMessages = Integer.parseInt(args[++i]);
                continue;
            }
            if("-initiator".equals(args[i])) {
                initiator = args[++i];
                continue;
            }
            if("-nodes".equals(args[i])) {
                minimumNumberOfNodes = Integer.parseInt(args[++i]);
                continue;
            }
        }

        // Hack to ensure that the Aramis protocol does not start until at least minimumNumberOfNodes have joined the current view
        if (minimumNumberOfNodes > 0)
            Aramis.minimumNodes = minimumNumberOfNodes;

        new InfiniteClients(propsFile, numberOfMessages, totalMessages, initiator).run();
    }

    public static final AtomicInteger msgsReceived = new AtomicInteger();
    private final String PROPERTIES_FILE;
    private final int NUMBER_MESSAGES_TO_SEND;
    private int TOTAL_NUMBER_OF_MESSAGES;
    private final int NUMBER_MESSAGES_PER_FILE = 1000;
    private final int LATENCY_INTERVAL = 5000;
    private final String INITIATOR;
    private final String PATH = "/work/a7109534/";
    private final List<Header> deliveredMessages = new ArrayList<Header>();
    private final short id = 1025;
    private ExecutorService outputThread = Executors.newSingleThreadExecutor();
    private JChannel channel;
    private int count = 1;
    private boolean startSending = false;
    private int completeMsgsReceived = 0;
    private long startTime;
    private boolean allMessagesReceived = false;
    private Future lastOutputFuture;

    private Map<Address, RMCastHeader> msgRecord = new HashMap<Address, RMCastHeader>();
    private boolean checkMissingSeq = false;

    public InfiniteClients(String propsFile, int numberOfMessages, int totalMessages, String initiator) {
        PROPERTIES_FILE = propsFile;
        NUMBER_MESSAGES_TO_SEND = numberOfMessages;
        TOTAL_NUMBER_OF_MESSAGES = totalMessages;
        INITIATOR = initiator;
    }

    public void run() throws Exception {
//        ExecutorService threadPool = Executors.newFixedThreadPool(25);

        System.out.println(PROPERTIES_FILE + " | Total Cluster Msgs " + TOTAL_NUMBER_OF_MESSAGES +
                " | Msgs to send " + NUMBER_MESSAGES_TO_SEND + " | Initiator " + INITIATOR);
        channel = new JChannel(PROPERTIES_FILE);
        channel.setReceiver(this);
        channel.connect("uperfBox");

        ClassConfigurator.add(id, TestHeader.class); // Add Header to magic map without adding to file
        System.out.println("Channel Created | lc := " + channel.getAddress());
        System.out.println("Number of message to send := " + NUMBER_MESSAGES_TO_SEND);

        Util.sleep(1000 * 30);
        int sentMessages = 0;
        startTime = System.nanoTime();

        if (channel.getAddress().toString().contains(INITIATOR))
            sendStartMessage(channel);

        while (true) {
            if (startSending) {
                AnycastAddress anycastAddress = new AnycastAddress(channel.getView().getMembers());
                final Message message = new Message(anycastAddress, sentMessages);
                message.putHeader(id, TestHeader.createTestMsg(sentMessages + 1));
                message.setBuffer(new byte[1000]);
//                threadPool.execute(new Runnable() {
//                    @Override
//                    public void run() {
//                        try {
//                            channel.send(message);
//                        } catch (Exception e) {
//                            System.out.println(e);
//                        }
//                    }
//                });
                channel.send(message);
                sentMessages++;

                if (sentMessages == NUMBER_MESSAGES_TO_SEND)
                    break;
            } else {
                Util.sleep(1);
            }
        }
        System.out.println("Sending finished! | Time Taken := " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));

        while (!allMessagesReceived || channel.getView().size() != completeMsgsReceived || (lastOutputFuture == null || !lastOutputFuture.isDone()))
            Util.sleep(100);

        System.out.println("Test Finished");
        System.exit(0);
    }

    public void sendStartMessage(JChannel channel) throws Exception {
        sendStatusMessage(channel, new TestHeader(TestHeader.START_SENDING));
    }

    private void sendCompleteMessage(JChannel channel) throws Exception {
        sendStatusMessage(channel, new TestHeader(TestHeader.SENDING_COMPLETE));
    }

    private void sendStatusMessage(JChannel channel, TestHeader header) throws Exception {
//        Message message = new Message(new AnycastAddress(channel.getView().getMembers()));
        Message message = new Message(null);
        message.putHeader(id, header);
        message.setFlag(Message.Flag.NO_TOTAL_ORDER);
        channel.send(message);
    }

    public void receive(Message msg) {
        long arrivalTime = System.nanoTime();
        long timeTaken = -1;

        final TestHeader testHeader = (TestHeader) msg.getHeader(id);
        if (testHeader != null) {
            byte type = testHeader.type;
            if (type == TestHeader.START_SENDING && !startSending) {
                startSending = true;
                System.out.println("Start sending msgs ------");
                return;
            } else if (type == TestHeader.SENDING_COMPLETE) {
                completeMsgsReceived++;

                if (msg.src() != null)
                    System.out.println("Complete message received from " + msg.src());

                int numberOfNodes = channel.getView().getMembers().size();
                if (completeMsgsReceived ==  numberOfNodes) {
                    System.out.println("Cluster finished! | Time Taken := " + TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
                }
                return;
            }
            timeTaken = arrivalTime - testHeader.timestamp;
        }

        synchronized (deliveredMessages) {
            short protocolId = (short) (PROPERTIES_FILE.equalsIgnoreCase("RMSysIntegrated.xml") ? 1008 : 58);
            Header header = msg.getHeader(protocolId);
            deliveredMessages.add(header);

            if (deliveredMessages.size() % NUMBER_MESSAGES_PER_FILE == 0) {
                final List<Header> outputHeaders = new ArrayList<Header>(deliveredMessages);
                deliveredMessages.clear();
                // Output using a single thread to ensure that this operation does not effect receiving messages
                lastOutputFuture = outputThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        writeHeadersToFile(outputHeaders);
                    }
                });
            }

            if (msg.src().equals(channel.getAddress()) && testHeader != null && testHeader.seq % LATENCY_INTERVAL == 0) {
                final long tt = timeTaken;
                lastOutputFuture = outputThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        writeLatencyToFile(testHeader.seq, tt);
                    }
                });
            }

            if (checkMissingSeq) {
                if (protocolId == 1008) {
                    RMCastHeader h = (RMCastHeader) header;
                    RMCastHeader oldHeader = msgRecord.put(h.getId().getOriginator(), h);
                    if (oldHeader != null && oldHeader.getId().getSequence() + 1 != h.getId().getSequence())
                        for (long i = oldHeader.getId().getSequence() + 1; i < h.getId().getSequence(); i++)
                            System.out.println("ERROR!!!!!!!! Sequence missing := " + i + " | from " + h.getId().getOriginator());

                    System.out.println("Msg received " + h.getId() + " | #" + (msgsReceived.intValue() + 1));
                } else {
                    System.out.println("Msg received | #  " + (msgsReceived.intValue() + 1));
                }
            }

            if (msgsReceived.incrementAndGet() == TOTAL_NUMBER_OF_MESSAGES) {
                try {
                    sendCompleteMessage(channel);
                    if (!deliveredMessages.isEmpty())
                        writeHeadersToFile(new ArrayList<Header>(deliveredMessages));
                } catch (Exception e) {
                }
            }
        }
    }

    public void viewAccepted(View view) {
        LogFactory.getLog(Aramis.class).warn("New View := " + view + " | " + Aramis.getClockTime() + " | Channel View := " + channel.getViewAsString());
    }

    private PrintWriter getPrintWriter(String path) {
        try {
            new File(PATH).mkdirs();
            return new PrintWriter(new BufferedWriter(new FileWriter(path, true)), true);
        } catch (Exception e) {
            System.out.println("Error: " + e);
            return null;
        }
    }

    private void writeHeadersToFile(List<Header> headers) {
        PrintWriter out = getPrintWriter(PATH + "DeliveredMessages" + channel.getAddress() + "-" + count + ".csv");
        for (Header header : headers)
            if (PROPERTIES_FILE.equalsIgnoreCase("RMSysIntegrated.xml"))
                out.println(((RMCastHeader) header).getId());
            else if (PROPERTIES_FILE.equalsIgnoreCase("toa.xml"))
                out.println(((ToaHeader)header).getMessageID());
        out.flush();

        int totalNumberOfRounds = (int) Math.round((TOTAL_NUMBER_OF_MESSAGES) / (double) NUMBER_MESSAGES_PER_FILE);
        if (count == totalNumberOfRounds) {
            allMessagesReceived = true;
            System.out.println("&&&&&&&& Final Count := " + count + " | totalNumberOfRounds := " + totalNumberOfRounds +
                    " | numberOfMessagesToSend := " + NUMBER_MESSAGES_TO_SEND + " | NUMBER_MESSAGES_PER_FILE := " + NUMBER_MESSAGES_PER_FILE);
        }
        System.out.println("Count == " + count + " | numberOfRounds := " + totalNumberOfRounds);
        count++;
    }

    private void writeLatencyToFile(int count, long timeTaken) {
        PrintWriter out = getPrintWriter(PATH + "Latencies" + channel.getAddress() + ".csv");
        out.println(count + "," + timeTaken);
    }

    public static class TestHeader extends Header {
        public static final byte START_SENDING = 1;
        public static final byte SENDING_COMPLETE = 2;
        public static final byte TEST_MSG = 3;

        private byte type;
        private int seq = -1;
        private long timestamp = -1;

        public TestHeader() {}

        public TestHeader(byte type) {
            this.type = type;
        }

        public TestHeader(byte type, int seq, long timestamp) {
            this.type = type;
            this.seq = seq;
            this.timestamp = timestamp;
        }

        public static TestHeader createTestMsg(int seq) {
            return new TestHeader(TEST_MSG, seq, System.nanoTime());
        }

        @Override
        public int size() {
            return Global.BYTE_SIZE;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            out.writeInt(seq);
            out.writeLong(timestamp);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            type = in.readByte();
            seq = in.readInt();
            timestamp = in.readLong();
        }

        @Override
        public String toString() {
            return "TestHeader{" +
                    "type=" + type +
                    ", seq=" + seq +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}