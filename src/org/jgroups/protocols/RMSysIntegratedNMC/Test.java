package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.*;
import org.jgroups.protocols.tom.ToaHeader;
import org.jgroups.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class Test extends ReceiverAdapter {
    public static void main(String[] args) throws Exception{
        String propsFile = "RMSysIntegrated.xml";
        int numberOfMessages = 100000;
        for (int i = 0; i < args.length; i++) {
            if("-config".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
            if("-nr-messages".equals(args[i])) {
                numberOfMessages = Integer.parseInt(args[++i]);
                continue;
            }
        }
        new Test(propsFile, numberOfMessages).run();
    }

    private final String PROPERTIES_FILE;
    private final int NUMBER_MESSAGES_TO_SEND;
    private final int NUMBER_MESSAGES_PER_FILE = 5000;
    private final int TIME_BETWEEN_REQUESTS = 1;
    private final String PATH = "/work/a7109534/";
    private final List<Header> deliveredMessages = new ArrayList<Header>();
    private ExecutorService outputThread = Executors.newSingleThreadExecutor();
    private Address localAddress;
    private int count = 1;

    public Test(String propsFile, int numberOfMessages) {
        PROPERTIES_FILE = propsFile;
        NUMBER_MESSAGES_TO_SEND = numberOfMessages;
    }

    public void run() throws Exception {
        System.out.println(PROPERTIES_FILE + " | " + NUMBER_MESSAGES_TO_SEND + " messages");
        final JChannel channel = new JChannel(PROPERTIES_FILE);
        channel.setReceiver(this);
        channel.connect("uperfBox");
        localAddress = channel.getAddress();
        System.out.println("Channel Created | lc := " + localAddress);
        System.out.println("Number of message to send := " + NUMBER_MESSAGES_TO_SEND);
        System.out.println("Time between each request := " + TIME_BETWEEN_REQUESTS);

        Util.sleep(1000 * 10);
        int count = 0;
        while (true) {
            AnycastAddress anycastAddress = new AnycastAddress(channel.getView().getMembers());
            Message message = new Message(anycastAddress, count);
            channel.send(message);
            Util.sleep(TIME_BETWEEN_REQUESTS);
            count++;

            if (count == NUMBER_MESSAGES_TO_SEND)
                break;
        }
        System.out.println("Sending finished!");
        Util.sleep(1000 * 60);
        channel.disconnect();
        System.exit(0);
    }

    public void receive(Message msg) {
        synchronized (deliveredMessages) {
            short protocolId = (short) (PROPERTIES_FILE.equals("RMsysIntegrated.xml") ? 1008 : 58);
            Header header = msg.getHeader(protocolId);
            deliveredMessages.add(header);

            if (deliveredMessages.size() % NUMBER_MESSAGES_PER_FILE == 0) {
                final List<Header> outputHeaders = new ArrayList<Header>(deliveredMessages);
                deliveredMessages.clear();
                // Output using a single thread to ensure that this operation does not effect receiving messages
                outputThread.submit(new Runnable() {
                    @Override
                    public void run() {
                        writeHeadersToFile(outputHeaders);
                    }
                });
            }
        }
    }

    public void viewAccepted(View view) {
        System.out.println("New View := " + view);
    }

    private PrintWriter getPrintWriter(Address localAddress, int count) {
        try {
            new File(PATH).mkdirs();

            return new PrintWriter(new BufferedWriter(new FileWriter(PATH + "DeliveredMessages" + localAddress + "-" + count + ".csv",
                    true)), true);
        } catch (Exception e) {
            System.out.println("Error: " + e);
            return null;
        }
    }

    private void writeHeadersToFile(List<Header> headers) {
        PrintWriter out = getPrintWriter(localAddress, count);
        for (Header header : headers)
            if (PROPERTIES_FILE.equals("RMsysIntegrated.xml"))
                out.println(((RMCastHeader) header).getId());
            else if (PROPERTIES_FILE.equals("toa.xml"))
                out.println(((ToaHeader)header).getMessageID());
        count++;
    }
}