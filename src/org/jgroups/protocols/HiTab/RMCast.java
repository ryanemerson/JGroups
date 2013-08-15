package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A protocol that handles the broadcasting of messages but provides no ordering
 *
 * @author ryan
 * @since 4.0
 */
public class RMCast extends Protocol {
    private final Map<MessageId, MessageRecord> messageRecords = new ConcurrentHashMap<MessageId, MessageRecord>();
    private final Map<MessageId, Message> receivedMessages = new ConcurrentHashMap<MessageId, Message>();
    private Address localAddress = null;
    private TimeScheduler timer;
    private View view;

    @Override
    public void init() throws Exception {
        timer = getTransport().getTimer();
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() {
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                RMCastHeader header = (RMCastHeader) message.getHeader(this.id);
                if (header == null)
                    return up_prot.up(event);

                receivedMessages.put(header.getId(), message); // Store actual message, need for retransmission
                handleMessage(event, header);
                return null;
            case Event.VIEW_CHANGE:
                view = (View) event.getArg();
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                broadcastMessage(event);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                return down_prot.down(event);
            default:
                return down_prot.down(event);
        }
    }

    public void handleMessage(Event event, RMCastHeader header) {
        final MessageRecord record;
        synchronized (messageRecords) {
            if (!messageRecords.containsKey(header.getId())) {
                up_prot.up(event); // Deliver to the above layer (Application or HiTab)

                record = new MessageRecord(header.getId(), header.getCopyTotal());
                messageRecords.put(header.getId(), record); // Copy received
            } else {
                record = messageRecords.get(header.getId());
            }
        }

        System.out.println(header);

        if (record.getLargestCopyReceived() == header.getCopyTotal() && record.crashNotified) {
            return;
        }

        if (header.getDisseminator() == header.getId().getOriginator() && !record.isCrashNotified()) {
            // TODO SEND NO CRASH NOTIFICATION
            // Effectively cancels the CrashedTimeout as nothing will happen once this is set to true
            record.setCrashNotified(true);
        }

        if (header.getCopy() == header.getCopyTotal()) {
            record.setLargestCopyReceived(header.getCopy());
        } else {

            if (!header.getId().getOriginator().equals(localAddress)
                    && header.getCopy() > record.getLargestCopyReceived()
                    || (header.getCopy() == record.getLargestCopyReceived() && (header.getDisseminator() == header
                    .getId().getOriginator() || getNodeSeniority(header.getDisseminator()) < getNodeSeniority(record.getBroadcastLeader())))) {
                responsivenessTimeout(record, header);
            }
        }
    }

    public void broadcastMessage(Event event) {
        final Message message = (Message) event.getArg();
        final NMCData data = getNMCData();
        final short headerId;

        short protocolId = ClassConfigurator.getProtocolId(HiTab.class);
        RMCastHeader existingHeader = (RMCastHeader) message.getHeader(protocolId);
        // If HiTab header is present, set the rmsys values in that header
        if (existingHeader != null) {
            if(((HiTabHeader)existingHeader).getType() != HiTabHeader.BROADCAST) {
                down_prot.down(event);
                return;
            }
            headerId = protocolId;
            existingHeader.setCopyTotal(data.getMessageCopies());
            existingHeader.setDisseminator(localAddress);
        } else {
            headerId = this.id;
            MessageId msgId = new MessageId(System.currentTimeMillis(), localAddress);
            final RMCastHeader header = new RMCastHeader(msgId, localAddress,
                    0, data.getMessageCopies());
            message.putHeader(this.id, header);
        }

        timer.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i <= data.getMessageCopies(); i++) {
                    ((RMCastHeader) message.getHeader(headerId)).setCopy(i);
                    down_prot.down(new Event(Event.MSG, message));
                    try {
                        Thread.sleep(Math.round(data.getEta()));
                    } catch (InterruptedException e) {
                        e.printStackTrace(); // TODO insert log
                    }
                }
            }
        });
    }

    public void disseminateMessage(final MessageRecord record, final RMCastHeader header) {
        timer.execute(new Runnable() {
            @Override
            public void run() {
                while (record.getLargestCopyReceived() < header.getCopyTotal() && record.getBroadcastLeader().equals(localAddress)) {
                    Message message = receivedMessages.get(header.getId());
                    int messageCopy = Math.max(record.getLastBroadcast() + 1, record.getLargestCopyReceived());
                    header.setDisseminator(localAddress);
                    header.setCopy(messageCopy);

                    short protocolId = ClassConfigurator.getProtocolId(HiTab.class);
                    RMCastHeader existingHeader = (RMCastHeader) message.getHeader(protocolId);
                    if (existingHeader != null)
                        message.putHeader(protocolId, header);
                    else
                        message.putHeader(id, header);

                    down_prot.down(new Event(Event.MSG, message));
                    // Update to show that the largestCopy received == last broadcast i.e we're the disseminator
                    record.setLargestCopyReceived(messageCopy);
                    record.setLastBroadcast(messageCopy);
                }
            }
        });
    }

    public void responsivenessTimeout(final MessageRecord record, final RMCastHeader header) {
        final NMCData data = getNMCData();
        record.setLargestCopyReceived(header.getCopy());
        record.setBroadcastLeader(header.getDisseminator());

        // Set Timeout 2 for n + w
        final int timeout = (int) Math.ceil(data.getEta() + data.getOmega());
        timer.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // TODO insert log statement
                }

                if (record.getLargestCopyReceived() < header.getCopyTotal()) {
                    record.setBroadcastLeader(null);
                    try {
                        Random r = new Random();
                        int eta = (int) data.getEta();
                        int ran = r.nextInt(eta < 1 ? eta + 1 : eta);
                        Thread.sleep(ran);
                    } catch (InterruptedException e) {
                        // TODO insert log statement
                    }
                    if (record.getLargestCopyReceived() < header.getCopyTotal() && record.getBroadcastLeader() == null) {
                        record.setBroadcastLeader(localAddress);
                        disseminateMessage(record, header);
                    }
                }
            }
        });
    }

    public NMCData getNMCData() {
        return (NMCData) down_prot.down(new Event(Event.USER_DEFINED, new HiTabEvent(HiTabEvent.GET_NMC_TIMES)));
    }

    public int getNodeSeniority(Address node) {
        if (node == null) {
            return Integer.MAX_VALUE;
        }
        return view.getMembers().indexOf(node);
    }

    final class MessageRecord {
        final private MessageId id;
        final private int totalCopies;
        private volatile int largestCopyReceived; // Largest copy received
        private volatile Address broadcastLeader; // Set to null if none
        private volatile int lastBroadcast;
        private volatile boolean crashNotified; // Not currently used for anything

        public MessageRecord(MessageId id, int totalCopies) {
            this(id, totalCopies, -1, null, -1, false);
        }

        public MessageRecord(MessageId id, int totalCopies, int largestCopyReceived, Address broadcastLeader,
                             int lastBroadcast, boolean crashNotified) {
            if (id == null)
                throw new IllegalArgumentException("A message records id feel cannot be null");

            this.id = id;
            this.totalCopies = totalCopies;
            this.largestCopyReceived = largestCopyReceived;
            this.broadcastLeader = broadcastLeader;
            this.lastBroadcast = lastBroadcast;
            this.crashNotified = crashNotified;
        }

        public MessageId getId() {
            return id;
        }

        public int getTotalCopies() {
            return totalCopies;
        }

        public int getLargestCopyReceived() {
            return largestCopyReceived;
        }

        public void setLargestCopyReceived(int largestCopyReceived) {
            this.largestCopyReceived = largestCopyReceived;
        }

        public Address getBroadcastLeader() {
            return broadcastLeader;
        }

        public void setBroadcastLeader(Address broadcastLeader) {
            this.broadcastLeader = broadcastLeader;
        }

        public int getLastBroadcast() {
            return lastBroadcast;
        }

        public void setLastBroadcast(int lastBroadcast) {
            this.lastBroadcast = lastBroadcast;
        }

        public boolean isCrashNotified() {
            return crashNotified;
        }

        public void setCrashNotified(boolean crashNotified) {
            this.crashNotified = crashNotified;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (crashNotified != that.crashNotified) return false;
            if (largestCopyReceived != that.largestCopyReceived) return false;
            if (lastBroadcast != that.lastBroadcast) return false;
            if (totalCopies != that.totalCopies) return false;
            if (broadcastLeader != null ? !broadcastLeader.equals(that.broadcastLeader) : that.broadcastLeader != null)
                return false;
            if (!id.equals(that.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + totalCopies;
            result = 31 * result + largestCopyReceived;
            result = 31 * result + (broadcastLeader != null ? broadcastLeader.hashCode() : 0);
            result = 31 * result + lastBroadcast;
            result = 31 * result + (crashNotified ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "id=" + id +
                    ", totalCopies=" + totalCopies +
                    ", largestCopyReceived=" + largestCopyReceived +
                    ", broadcastLeader=" + broadcastLeader +
                    ", lastBroadcast=" + lastBroadcast +
                    ", crashNotified=" + crashNotified +
                    '}';
        }
    }
}
