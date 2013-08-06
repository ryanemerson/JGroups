package org.jgroups.protocols.HiTab;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;

/**
 * // TODO: Document this
 *
 * @author ryanemerson
 * @since 4.0
 */
public class HiTab extends Protocol {

    public HiTab() {
    }

    @Override
    public void init() throws Exception{
        System.out.println("Init");
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() {
    }

    @Override
    public Object down(Event evt) {
        System.out.println("Message passed down");
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        System.out.println("\nMessage passed up");
        return up_prot.up(evt);
    }
}