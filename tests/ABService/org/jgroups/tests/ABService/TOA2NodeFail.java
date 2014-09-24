package org.jgroups.tests.ABService;

import org.jgroups.*;
import org.jgroups.protocols.tom.ToaHeader;
import org.jgroups.util.Util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A test to see if the TOA protocol fails when utilised with only two destinations.
 * Resolved in JGRP-1785.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class TOA2NodeFail extends ReceiverAdapter {
    public static void main(String[] args) {
        try {
            new TOA2NodeFail().run("toa.xml");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private ToaHeader lastHeader = null;
    private Map<Address, Integer> countRecord = new HashMap<Address, Integer>();
    private Address localAddress = null;
    private PrintWriter out = null;
    private List<ToaHeader> receivedMessages = new ArrayList<ToaHeader>();

    public void run(String propsFile) throws Exception {
        System.out.println(propsFile);
        JChannel channel = new JChannel(propsFile);
        channel.setReceiver(this);
        channel.connect("uperfBox");
        localAddress = channel.getAddress();

        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter("DeliveredMessages" + localAddress + ".csv",
                    true)), true);
        } catch (Exception e) {
        }

        Util.sleep(1000 * 10);
        int count = 0;
        while (true) {
            AnycastAddress anycastAddress = new AnycastAddress(channel.getView().getMembers());
            Message message = new Message(anycastAddress, new Integer(count));
            channel.send(message);
            Util.sleep(1);
            count++;

            if (count > 20000)
                break;
        }
        System.out.println("Sending finished!");

        Util.sleep(1000 * 20);

        for (ToaHeader h : receivedMessages) {
            out.println(h);
        }
    }

    public void receive(Message msg) {
        ToaHeader thisHeader = (ToaHeader) msg.getHeader((short) 58);
        receivedMessages.add(thisHeader);
    }
}