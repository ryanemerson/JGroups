package org.jgroups.tests.probing_validation;

/**
 * Entry point to the probingvalidation experiments
 *
 * @author a7109534
 * @since 4.0
 */
public class ExperimentMain {
    public static void main(String[] args) throws Exception {
        final short MASTER_HEADER_ID = 1025;
        final short PROBING_HEADER_ID = 1026;
        String propsFile = "ProbingValidation.xml";
        int numberOfProbingNodes = 5; // #Nodes that will probe during each round
        int numberOfRounds = 100; // #Msgs to be executed by this node
        int roundDuration = 1000; // #Duration of each round in milliseconds
        boolean masterNode = false; // Is this node the master node

        for (int i = 0; i < args.length; i++) {
            if ("-p".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
            if ("-pn".equals(args[i])) {
                numberOfProbingNodes = Integer.parseInt(args[++i]);
                continue;
            }
            if ("-rd".equals(args[i])) {
                roundDuration = Integer.parseInt(args[++i]);
                continue;
            }
            if ("-nr".equals(args[i])) {
                numberOfRounds = Integer.parseInt(args[++i]);
                continue;
            }
            if ("-m".equals(args[i])) {
                masterNode = true;
            }
        }

        if (masterNode)
            new MasterNode(MASTER_HEADER_ID, propsFile, numberOfProbingNodes).run();
        else
            new ProbingNode(MASTER_HEADER_ID, PROBING_HEADER_ID, propsFile, numberOfProbingNodes, numberOfRounds, roundDuration).run();
    }
}
