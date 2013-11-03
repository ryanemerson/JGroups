package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author ryan
 * @since 4.0
 */
public class DeliveryManager {
    private final SortedSet<MessageRecord> deliverySet;
    private final Set<Message> singleDestinationSet;
    private final Map<MessageId, Message> messageStore;
    private final ViewManager viewManager;
    private final AtomicInteger lastDelivered;
    private Address localAddress;

    public DeliveryManager(ViewManager viewManager, Address localAddress) {
        this.viewManager = viewManager;
        this.localAddress = localAddress;

        deliverySet = new TreeSet<MessageRecord>();
        singleDestinationSet = new HashSet<Message>();
        messageStore = new ConcurrentHashMap<MessageId, Message>();
        lastDelivered = new AtomicInteger();
    }

    public void addMessageToDeliver(DecoupledHeader header, Message message) {
        MessageRecord record = new MessageRecord(header, message);
        readyToDeliver(record);
        synchronized (deliverySet) {
            deliverySet.add(record);

            if (deliverySet.first().isDeliverable) {
                if (deliverySet.size() != 1) {
                    recheckRecords();
                }
                deliverySet.notify();
            }
        }
    }

    public void addSingleDestinationMessage(Message msg) {
        synchronized (deliverySet) {
            singleDestinationSet.add(msg);
            deliverySet.notify();
        }
    }

    public void addMessageToStore(MessageId id, Message message) {
        message.setSrc(id.getOriginator());
        messageStore.put(id, message);
    }

    public Message getMessageFromStore(MessageId id) {
        return messageStore.get(id);
    }

    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> msgsToDeliver = new LinkedList<Message>();
        synchronized (deliverySet) {
            while (deliverySet.isEmpty()) {
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

    private void recheckRecords() {
        for (MessageRecord record: deliverySet) {
            readyToDeliver(record);
        }
    }

    private void readyToDeliver(MessageRecord record) {
        DecoupledHeader header = record.header;
        MessageInfo messageInfo = header.getMessageInfo();
        // The ordering sequence of the last delivered message.  If a message is received that is +1 to this, we can deliver immediately
        // otherwise we need to check the ordering list
        if (messageInfo.getOrdering() == lastDelivered.intValue() + 1) {
            record.isDeliverable = true;
            return;
        }

        long thisMessagesOrder = messageInfo.getOrdering();
        for (MessageInfo info : header.getOrderList()) {
            if (viewManager.containsAddress(info, localAddress)) {
                long olderMessageOrdering = info.getOrdering();
                if (olderMessageOrdering > lastDelivered.intValue() && olderMessageOrdering < thisMessagesOrder) {
                    record.isDeliverable = false;
                    return;
                }
            }
        }
        record.isDeliverable = true;
    }

    private class MessageRecord implements Comparable<MessageRecord> {
        private DecoupledHeader header;
        private Message message;
        private boolean isDeliverable;

        private MessageRecord(DecoupledHeader header, Message message) {
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
    }
}