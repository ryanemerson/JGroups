package org.jgroups.tests.ABService;

import org.jgroups.JChannel;
import org.jgroups.protocols.aramis.Aramis;

/**
 * Class that provides a 'dumb' application, that simply runs a network stack.  Useful for simulating ordering box members
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class AbaaSBoxMember {
    public static void main(String[] args) throws Exception {
        String properties = "";
        String channel = "";
        int minimumNumberOfNodes = -1;

        for (int i=0; i < args.length; i++) {
            if ("-props".equals(args[i])) {
                properties = args[++i];
                continue;
            }
            if ("-channel".equals(args[i])) {
                channel = args[++i];
            }
            if("-nodes".equals(args[i])) {
                minimumNumberOfNodes = Integer.parseInt(args[++i]);
                continue;
            }
        }

        // Hack to ensure that the Aramis protocol does not start until at least minimumNumberOfNodes have joined the current view
        if (minimumNumberOfNodes > 0)
            Aramis.minimumNodes = minimumNumberOfNodes;

        JChannel jChannel = new JChannel(properties);
        jChannel.connect(channel);
    }
}
