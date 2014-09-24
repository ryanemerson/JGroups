package org.jgroups.protocols.abaas;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class, and associated inner classes, that are responsible for ensuring that abcast messages are delivered by client nodes
 * in the correct total order.
 *
 * @author ryan
 * @since 4.0
 */
public class DeliveryManager {
    private final SortedSet<MessageRecord> deliverySet;
    private final Set<Message> singleDestinationSet;
    private final ViewManager viewManager;
    private final AtomicLong lastDelivered;
    private final Log log;
    private Address localAddress;

    public DeliveryManager(Log log, ViewManager viewManager) {
        this.log = log;
        this.viewManager = viewManager;

        deliverySet = Collections.synchronizedSortedSet(new TreeSet<MessageRecord>());
        singleDestinationSet = new HashSet<Message>();
        lastDelivered = new AtomicLong();
    }

    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void addSingleDestinationMessage(Message msg) {
        synchronized (deliverySet) {
            singleDestinationSet.add(msg);
            deliverySet.notify();
        }
    }

    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> msgsToDeliver = new LinkedList<Message>();
        synchronized (deliverySet) {
            while (deliverySet.isEmpty() && singleDestinationSet.isEmpty()) {
                deliverySet.wait();
            }

            if (!singleDestinationSet.isEmpty()) {
                msgsToDeliver.addAll(singleDestinationSet);
                singleDestinationSet.clear();
                return msgsToDeliver;
            }

            if (!deliverySet.first().isDeliverable) {
                deliverySet.wait();
            }

            if (!singleDestinationSet.isEmpty()) {
                msgsToDeliver.addAll(singleDestinationSet);
                singleDestinationSet.clear();
            }

            Iterator<MessageRecord> iterator = deliverySet.iterator();
            while (iterator.hasNext()) {
                MessageRecord record = iterator.next();
                if (record.isDeliverable) {
                    msgsToDeliver.add(record.message);
                    viewManager.removeOldViews(record.header.getMessageInfo());
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        return msgsToDeliver;
    }

    public void addMessageToDeliver(AbaaSHeader header, Message message, boolean local) {
        if (header.getMessageInfo().getOrdering() <= lastDelivered.longValue()) {
            if (log.isDebugEnabled())
                log.debug("Message already received or Missed! | " + header.getMessageInfo().getOrdering() + " | local := " + local);
            return;
        }

        if (log.isTraceEnabled())
            log.trace("Add message to deliver | " + header.getMessageInfo() + " | lastDelivered := " + lastDelivered.longValue());

        MessageRecord record = new MessageRecord(header, message);
        synchronized (deliverySet) {
            readyToDeliver(record);
            deliverySet.add(record);
            MessageRecord firstRecord = deliverySet.first();
            if (firstRecord.isDeliverable) {
                recheckRecords(firstRecord);
                deliverySet.notify();
            }
        }
    }

    private boolean readyToDeliver(MessageRecord record) {
        AbaaSHeader header = record.header;
        MessageInfo messageInfo = header.getMessageInfo();
        long thisMessagesOrder = messageInfo.getOrdering();
        long lastDelivery = lastDelivered.longValue();

        long lastOrdering = viewManager.getLastOrdering(messageInfo, localAddress);
        if (lastOrdering < 0 || (lastOrdering > 0 && lastOrdering <= lastDelivery)) {
            // lastOrder has already been delivered, so this message is deliverable
            record.isDeliverable = true;
            lastDelivered.set(thisMessagesOrder);
            return true;
        }
        if (log.isTraceEnabled())
            log.trace("Previous message not received | " + thisMessagesOrder + " | require " + lastOrdering);
        return false;
    }

    private void recheckRecords(MessageRecord record) {
        Set<MessageRecord> followers = deliverySet.tailSet(record);
        for (MessageRecord r : followers) {
            if (r.equals(record))
                continue;
            if (!readyToDeliver(r))
                return;
        }
    }

    class MessageRecord implements Comparable<MessageRecord> {
        final private AbaaSHeader header;
        final private Message message;
        private volatile boolean isDeliverable;

        private MessageRecord(AbaaSHeader header, Message message) {
            this.header = header;
            this.message = message;
            this.isDeliverable = false;
            this.message.setSrc(header.getMessageInfo().getId().getOriginator());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (header != null ? !header.equals(that.header) : that.header != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return header != null ? header.hashCode() : 0;
        }

        @Override
        public int compareTo(MessageRecord nextRecord) {
            long thisOrdering = header.getMessageInfo().getOrdering();
            long nextOrdering = nextRecord.header.getMessageInfo().getOrdering();

            if (this.equals(nextRecord))
                return 0;
            else if (thisOrdering > nextOrdering)
                return 1;
            else
                return -1;
        }

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "header=" + header +
                    ", isDeliverable=" + isDeliverable +
                    '}';
        }
    }
}