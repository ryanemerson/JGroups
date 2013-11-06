package org.jgroups.protocols.DecoupledBroadcast;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;

import java.util.*;

public class ViewManager {
    private final Map<ViewId, View> viewStore;
    private volatile View currentView;

    public ViewManager() {
        this.viewStore = new HashMap<ViewId, View>();
    }

    public void setCurrentView(View view) {
        this.currentView = view;
        addView(view);
    }

    private void addView(View view) {
        synchronized (viewStore) {
            viewStore.put(view.getViewId(), view);
        }
    }

    public void removeOldViews(MessageInfo messageInfo) {
        ViewId id = messageInfo.getViewId();
        if (id.equals(currentView.getViewId()))
            return;

        synchronized (viewStore) {
            Iterator<ViewId> i = viewStore.keySet().iterator();
            while (i.hasNext()) {
                if (id.compareTo(i.next()) > 0)
                    i.remove();
            }
        }
    }

    public boolean containsAddress(MessageInfo messageInfo, Address address) {
        return getDestinations(messageInfo).contains(address);
    }

    public List<Address> getDestinations(MessageInfo messageInfo) {
        ViewId id = messageInfo.getViewId();
        if (currentView.getViewId().equals(id)) {
            return getAddresses(currentView, messageInfo.getDestinations());
        } else {
            View oldView = viewStore.get(messageInfo.getViewId());
            return getAddresses(oldView, messageInfo.getDestinations());
        }
    }

    public byte[] getDestinationsAsByteArray(Collection<Address> addresses) {
        if (addresses.size() > Byte.MAX_VALUE)
            throw new IllegalArgumentException("Number of addresses cannot be greater than " + Byte.MAX_VALUE);

        byte[] destinations = new byte[addresses.size()];
        int index = 0;
        for (Address address : addresses)
            destinations[index++] = (byte) currentView.getMembers().indexOf(address);
        return destinations;
    }

    public long getLastOrdering(MessageInfo messageInfo, Address address) {
        List<Address> destinations = getDestinations(messageInfo);
        int addressIndex = destinations.indexOf(address);
        if (addressIndex >= 0)
            return messageInfo.getLastOrderSequence()[addressIndex];
        else
            return -1;
    }

    private List<Address> getAddresses(View view, byte[] indexes) {
        if (view == null)
            throw new IllegalArgumentException("View cannot be null");

        List<Address> addresses = new ArrayList<Address>();
        for (byte index : indexes) {
            addresses.add(view.getMembers().get(index));
        }
        return addresses;
    }
}