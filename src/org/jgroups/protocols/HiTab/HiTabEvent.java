package org.jgroups.protocols.HiTab;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class HiTabEvent {
    public static final int NMC_READY = 1; // arg = View (current view of the cluster from this node
    public static final int CLOCK_SYNCHRONISED = 2; // arg = null
    public static final int GET_CLOCK_TIME = 3; // arg = null --> Long
    public static final int GET_CLOCK_ERROR = 4; // arg = null --> Long
    public static final int GET_NMC_TIMES = 5; // arg = null --> NMCData
    public static final int BROADCAST_COMPLETE = 6; // arg = MessageId --> Boolean
    public static final int COLLECT_GARBAGE = 7; // arg = List<MessageId>

    private final int type;
    private final Object arg;

    public HiTabEvent(int type) {
        this.type = type;
        this.arg = null;
    }

    public HiTabEvent(int type, Object arg) {
        this.type = type;
        this.arg = arg;
    }

    public final int getType() {
        return type;
    }

    public Object getArg() {
        return arg;
    }

    public static String type2String(int t) {
        switch(t) {
            case NMC_READY:	                return "NMC_READY";
            case CLOCK_SYNCHRONISED:        return "CLOCK_SYNCHRONISED";
            case GET_CLOCK_TIME:            return "GET_CLOCK_TIME";
            case GET_CLOCK_ERROR:           return "GET_CLOCK_ERROR";
            case GET_NMC_TIMES:             return "GET_NMC_TIMES";
            case BROADCAST_COMPLETE:        return "BROADCAST_COMPLETE";
            case COLLECT_GARBAGE:           return "COLLECT_GARBAGE";
            default:                        return "UNDEFINED(" + t + ")";
        }
    }

    @Override
    public String toString() {
        return "HiTabEvent{" +
                "type=" + type2String(type) +
                ", arg=" + arg +
                '}';
    }
}