package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class Test extends ReceiverAdapter {
    public static void main(String[] args) {
        try {
            new Test().run("RMSysIntegrated.xml");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage() + " | " + e.getCause());
        }
    }

    private Address localAddress = null;

    public void run(String propsFile) throws Exception {
        System.out.println(propsFile);
        JChannel channel = new JChannel(propsFile);
        channel.setReceiver(this);
        channel.connect("uperfBox");
        localAddress = channel.getAddress();

        System.out.println("Channel Created | lc := " + channel.getAddress());

        Util.sleep(1000 * 30);
        int count = 0;
        while (true) {
            AnycastAddress anycastAddress = new AnycastAddress(channel.getView().getMembers());
            Message message = new Message(anycastAddress, count);
            channel.send(message);
            Util.sleep(5);
            count++;

            if (count > 5)
                break;
        }
//        System.out.println("Sending finished!");
//
//        Util.sleep(1000 * 20);
        // TODO insert output code etc
    }

    public void receive(Message msg) {
        System.out.println("Message received from := " + msg.getSrc());
    }

    public void viewAccepted(View view) {
        System.out.println("New View := " + view);
    }
}