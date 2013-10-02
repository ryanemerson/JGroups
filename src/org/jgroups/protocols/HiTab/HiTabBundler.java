package org.jgroups.protocols.HiTab;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
* // TODO: Document this
*
* @author ryan
* @since 4.0
*/
public class HiTabBundler extends Protocol {
    private int capacity = 2000;
    private int maxBundleSize = 60000; // Max bundle size in bytes
    private int bundlerTimeout = 5; // Bundler timeout in milliseconds
    private final BlockingQueue<Message> buffer = new ArrayBlockingQueue<Message>(capacity, true);
    private final AtomicLong bufferSize = new AtomicLong(); // Current Size of the buffer in bytes;
    private final Map<Address, Message> messageStore = new HashMap<Address, Message>(); // Temp message store to hold
    private Address localAddress = null;
    private TimeScheduler timer;
    private Future timeoutTask;
    final ReentrantLock lock = new ReentrantLock(true);

    public HiTabBundler() {
    }

    @Override
    public void init() throws Exception{
        timer = getTransport().getTimer();
    }

    @Override
    public void start() throws Exception {        super.start();
    }

    @Override
    public void stop() {
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                HiTabBundlerHeader header = (HiTabBundlerHeader) message.getHeader(id);
                if (header == null)
                    break;

                HiTabHeader h = (HiTabHeader) message.getHeader((short)1004);
                unBundle(message.getBuffer());
                return null;
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                if (message.isFlagSet(Message.Flag.DONT_BUNDLE))
                    break;

                lock.lock();
                try {
                    addMessage(message);
                } catch (Exception e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                } finally {
                    lock.unlock();
                }
                return null;
            case Event.SET_LOCAL_ADDRESS:
                localAddress = (Address) event.getArg();
                break;
        }
        return down_prot.down(event);
    }

    private void addMessage(Message message) throws Exception {
        checkLength(message);

        if ((bufferSize.longValue() + message.size()) >= maxBundleSize)
            emptyBuffer();

        // Necessary as checking .remainingCapacity doesn't ensure that the message will be added due to multiple threads
        if (!buffer.offer(message)) {
            emptyBuffer();
            buffer.put(message);
        }
        bufferSize.addAndGet(message.size());

        if (buffer.size() == 1)
            timeoutTask = timer.schedule(new BundleTimeout(), bundlerTimeout, TimeUnit.MILLISECONDS);
    }

    private void emptyBuffer() {
        lock.lock();
        timeoutTask.cancel(false);
        List<Message> messageBundle = new ArrayList<Message>();
        buffer.drainTo(messageBundle);
        long bundleSize = bufferSize.getAndSet(0);
        processBuffer(bundleSize, messageBundle);
        lock.unlock();
    }

    private void processBuffer(long bundleSize, List<Message> messageBundle) {
        Map<Address, List<Message>> tempMap = new HashMap<Address, List<Message>>();
        for (Message message : messageBundle) {
            List<Message> messages = tempMap.get(message.getDest());
            if (messages == null) {
                messages = new ArrayList<Message>();
                tempMap.put(message.getDest(), messages);
            }
            messages.add(message);
        }

        for (Address destination : tempMap.keySet()) {
            List<Message> messages = tempMap.get(destination);
            if (messages.size() == 1)
                down_prot.down(new Event(Event.MSG, messages.get(0)));
            else
                sendBundle(bundleSize, destination, messages);
        }
    }

    private void sendBundle(long bundleSize, Address destination, List<Message> messageBundle) {
        ExposedByteArrayOutputStream outStream = new ExposedByteArrayOutputStream((int)(bundleSize + 50));
        ExposedDataOutputStream dataOutStream = new ExposedDataOutputStream(outStream);

        // TODO update these values so multicast is dynamic etc
        try {
            TP.writeMessageList(destination, localAddress, "", messageBundle, dataOutStream, true, id);
            Buffer buffer = new Buffer(outStream.getRawBuffer(), 0, outStream.size());
            Message bundleMessage = new Message(destination, buffer);
            bundleMessage.putHeader(id, new HiTabBundlerHeader());
            Event sendBundle = new Event(Event.MSG, bundleMessage);
            down_prot.down(sendBundle);
        }
        catch (Exception e) {
            System.err.println(e);
        }
    }

    private void unBundle(byte[] buffer) {
        List<Message> messages;
        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buffer));
        try {
            short version=in.readShort();
            byte flags=in.readByte();
            messages = TP.readMessageList(in, id);
        } catch (Exception e) {
            System.out.println("UNABLE TO CREATE MESSAGE LIST");
            System.out.println(e);
            return;
        }

        for (Message message : messages)
            up_prot.up(new Event(Event.MSG, message));
    }

    private void checkLength(Message message) {
        if (message.size() > maxBundleSize)
            throw new IllegalArgumentException("Message size cannot exceed the max_bundle_size");
    }

    private class BundleTimeout implements Runnable {
        public void run() {
            emptyBuffer();
        }
    }

    protected static class HiTabBundlerHeader extends Header {
        public HiTabBundlerHeader() {}

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
        }
    }
}