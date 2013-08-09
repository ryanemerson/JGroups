package org.jgroups.protocols.HiTab;

/**
 * A class that contains the values calculated by the NMC
 *
 * @author ryan
 * @since 4.0
 */
public class NMCData {
    private final double eta; // Milliseconds
    private final int messageCopies;
    private final int omega; // Milliseconds
    private final int capD; // Milliseconds
    private final int capS; // Milliseconds
    private final int xMax; // Milliseconds

    public NMCData(double eta, int messageCopies, int omega, int capD, int capS, int xMax) {
        this.eta = eta;
        this.messageCopies = messageCopies;
        this.omega = omega;
        this.capD = capD;
        this.capS = capS;
        this.xMax = xMax;
    }

    public double getEta() {
        return eta;
    }

    public int getOmega() {
        return omega;
    }

    public int getMessageCopies() {
        return messageCopies;
    }

    public int getCapD() {
        return capD;
    }

    public int getCapS() {
        return capS;
    }

    public int getXMax() {
        return xMax;
    }

    @Override
    public String toString() {
        return "NMCData{" +
                "eta=" + eta +
                ", messageCopies=" + messageCopies +
                ", omega=" + omega +
                ", capD=" + capD +
                ", capS=" + capS +
                ", xMax=" + xMax +
                '}';
    }
}