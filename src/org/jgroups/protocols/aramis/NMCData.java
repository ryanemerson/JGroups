package org.jgroups.protocols.aramis;

import org.jgroups.Global;
import org.jgroups.util.Streamable;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * A class that contains the values calculated by the NMC.
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
    private long timestamp = -1; // The time at which this object was created. Only used locally, not serialised

    public NMCData() {
    }

    public NMCData(NMCData data, int xMax, long timestamp) {
        this(data.eta, data.messageCopies, data.omega, data.capD, data.capS, xMax, timestamp);
    }

    public NMCData(int eta, int messageCopies, int omega, int capD, int capS, int xMax, long timestamp) {
        this.eta = eta;
        this.messageCopies = messageCopies;
        this.omega = omega;
        this.capD = capD;
        this.capS = capS;
        this.xMax = xMax;
        this.timestamp = timestamp;
    }

    public NMCData(int eta, int messageCopies, int omega, int capD, int capS, int xMax) {
        this(eta, messageCopies, omega, capD, capS, xMax, -1);
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

    public long getTimestamp() {
        return timestamp;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NMCData nmcData = (NMCData) o;

        if (capD != nmcData.capD) return false;
        if (capS != nmcData.capS) return false;
        if (eta != nmcData.eta) return false;
        if (messageCopies != nmcData.messageCopies) return false;
        if (omega != nmcData.omega) return false;
        if (timestamp != nmcData.timestamp) return false;
        if (xMax != nmcData.xMax) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = eta;
        result = 31 * result + messageCopies;
        result = 31 * result + omega;
        result = 31 * result + capD;
        result = 31 * result + capS;
        result = 31 * result + xMax;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
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
                ", timestamp=" + timestamp +
                '}';
    }
}