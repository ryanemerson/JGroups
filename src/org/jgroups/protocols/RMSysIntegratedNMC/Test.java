package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class Test extends ReceiverAdapter {
    public static void main(String[] args) throws Exception{
//        try {
            new Test().run("RMSysIntegrated.xml");
//        } catch (Exception e) {
//            System.out.println("Error: " + e.getMessage() + " | " + e.getCause());
//        }
    }

    private final int NUMBER_MESSAGES_TO_SEND = 100000;
    private final int NUMBER_MESSAGES_PER_FILE = 10000;
    private final int TIME_BETWEEN_REQUESTS = 10;
//    private final String PATH = "/work/a7109534/";
    private final String PATH = "";
    private final List<RMCastHeader> deliveredMessages = new ArrayList<RMCastHeader>();
    private ExecutorService outputThread = Executors.newSingleThreadExecutor();
    private Address localAddress;
    private int count = 1;

    public void run(String propsFile) throws Exception {
        System.out.println(propsFile);
        final JChannel channel = new JChannel(propsFile);
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
//        writeHeadersToFile(new ArrayList<RMCastHeader>(deliveredMessages));
        Util.sleep(1000 * 10);
        channel.disconnect();
        System.exit(0);
    }

    public void receive(Message msg) {
        synchronized (deliveredMessages) {
            RMCastHeader header = (RMCastHeader) msg.getHeader((short) 1008);
            deliveredMessages.add(header);

            if (deliveredMessages.size() % NUMBER_MESSAGES_PER_FILE == 0) {
                final List<RMCastHeader> outputHeaders = new ArrayList<RMCastHeader>(deliveredMessages);
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
            return new PrintWriter(new BufferedWriter(new FileWriter(PATH + "DeliveredMessages" + localAddress + "-" + count + ".csv",
                    true)), true);
        } catch (Exception e) {
            System.out.println("Error: " + e);
            return null;
        }
    }

    private void writeHeadersToFile(List<RMCastHeader> headers) {
        PrintWriter out = getPrintWriter(localAddress, count);
        for (RMCastHeader header : headers)
            out.println(header.getId() + " | DT := " + calculateDeliveryTime(header));
        count++;
    }

    private long calculateDeliveryTime(RMCastHeader header) {
        NMCData data = header.getNmcData();
        long startTime = header.getId().getTimestamp();
        long delay = TimeUnit.MILLISECONDS.toNanos(Math.max(data.getCapD(), data.getXMax() + data.getCapS()));
        return startTime + delay + 1000000;
    }
}