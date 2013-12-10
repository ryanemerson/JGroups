package org.jgroups.protocols.RMSysIntegratedNMC;

import org.jgroups.Global;
import org.jgroups.util.Streamable;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * A class that contains the values calculated by the NMC
 *
 * @author ryan
 * @since 4.0
 */
public class NMCData implements Streamable {
    private int eta; // Milliseconds
    private int messageCopies;
    private int omega; // Milliseconds
    private int capD; // Milliseconds
    private int capS; // Milliseconds
    private int xMax; // Milliseconds

    public NMCData() {
    }

    public NMCData(int eta, int messageCopies, int omega, int capD, int capS, int xMax) {
        this.eta = eta;
        this.messageCopies = messageCopies;
        this.omega = omega;
        this.capD = capD;
        this.capS = capS;
        this.xMax = xMax;
    }

    public int getEta() {
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

    public int size() {
        return Global.DOUBLE_SIZE + (Global.INT_SIZE * 5);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeInt(eta);
        out.writeInt(omega);
        out.writeInt(messageCopies);
        out.writeInt(capD);
        out.writeInt(capS);
        out.writeInt(xMax);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        eta = in.readInt();
        omega = in.readInt();
        messageCopies = in.readInt();
        capD = in.readInt();
        capS = in.readInt();
        xMax = in.readInt();
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