package org.jgroups.protocols.HiTab;

import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * // TODO: Document this
 *
 * @author ryanemerson
 * @since 4.0
 */
public class Test {

    static String print(View view) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        sb.append(view.getClass().getSimpleName() + ": ").append(view.getViewId()).append(": ");
        for(Address mbr: view.getMembers()) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(mbr);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception{
        String props = "/home/ryan/workspace/JGroups/conf/HiTab.xml";
        System.out.println("Props Location := " + props);

        final JChannel ch = new JChannel(props);
//        ch.setName("Test Channel");
        ch.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg) {
                Address sender = msg.getSrc();
                System.out.println("<< " + msg.getObject() + " from " + sender.toString());
            }

            public void viewAccepted(View new_view) {
                System.out.println("SDS" + print(new_view));
            }
        });

        ch.addChannelListener(new ChannelListener() {
            @Override
            public void channelConnected(Channel channel) {
                // TODO: Customise this generated block
            }

            @Override
            public void channelDisconnected(Channel channel) {
                // TODO: Customise this generated block
            }

            @Override
            public void channelClosed(Channel channel) {
                // TODO: Customise this generated block
            }
        });

        ch.connect("test");
        for(;;) {
            String line= Util.readStringFromStdin(": ");
            ch.send(null, line);
        }
    }
}