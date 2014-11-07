package org.jgroups.protocols.aramis;

/**
 * // TODO: Document this
 *
 * @author a7109534
 * @since 4.0
 */
public interface NMC {

    public NMCData getData();

    public void setActiveNodes(int numberOfNodes);

    public boolean initialProbesReceived();

    public void receiveProbe(RMCastHeader header);

    public double calculateR() throws Exception;
}
