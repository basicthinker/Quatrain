/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.stanzax.quatrain.io.ByteArrayOutputStream;
import org.stanzax.quatrain.io.InputChannelBuffer;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.SocketChannelPool;
import org.stanzax.quatrain.io.Writable;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 * 
 */
public class MrClient {

    public MrClient(InetAddress host, int port, WritableWrapper wrapper, long timeout) 
    throws IOException {
        this(new InetSocketAddress(host, port), wrapper, timeout);
    }
    
    public MrClient(InetSocketAddress address, WritableWrapper wrapper, long timeout) 
    throws IOException {
        this.address = address;
        this.writable = wrapper;
        this.timeout = timeout;
        
        listener  = new Listener();
        listener.setDaemon(true);
        listener.start();   
    }

    /**
     * Set binded server for this client
     * 
     * @param address
     *            Socket address of target server
     */
    public void useRemote(InetSocketAddress address) {
        this.address = address;
    }
    
    public InetSocketAddress getRemoteSocketAddress() {
        return this.address;
    }
    
    public ReplySet invoke(Type returnType, String procedureName) {
        return invoke(timeout, returnType, procedureName, new Object[0]);
    }
    
    public ReplySet invoke(int timeout, Type returnType, String procedureName) {
        return invoke(timeout, returnType, procedureName, new Object[0]);
    }
    
    public ReplySet invoke(Type returnType, String procedureName,
            Object...parameters) {
        return invoke(timeout, returnType, procedureName, parameters);
    }
    
    public ReplySet invoke(long timeout, Type returnType, String procedureName,
            Object...parameters) {
        int callID = counter.incrementAndGet();
        // Early create and register result set for awaiting reply
        ReplySet results = new ReplySet(writable.newInstance(returnType), timeout);
        results.register(callID);
        try {
            SocketChannel channel = channelPool.getSocketChannel(address);
            ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(128);
            DataOutputStream dataOut = new DataOutputStream(arrayOut);
            dataOut.writeInt(0); // occupied ahead for length
            writable.valueOf(callID).write(dataOut); //write call ID
            writable.valueOf(procedureName).write(dataOut); //write the procedure name
            for (Object parameter : parameters) { //write procedure parameters
                writable.valueOf(parameter).write(dataOut);
            }
            dataOut.flush();
            // Allocate byte buffer and insert ahead data length
            int dataLength = dataOut.size();
            ByteBuffer requestBuffer = ByteBuffer.wrap(arrayOut.getByteArray(), 
                    0, dataLength);
            requestBuffer.putInt(0, dataLength - 4);
            // Send RPC request
            assert(channel.isBlocking());
            synchronized(channel) { // channel in blocking mode
                channel.write(requestBuffer);
            }
            if (Log.DEBUG) Log.action("Send request with .callID .length", 
                    callID, dataLength);
            // Register for reply
            channel.configureBlocking(false);
            listener.register(channel, SelectionKey.OP_READ, 
                    new InputChannelBuffer(channel));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /** Hold a socket address of the target server */
    private InetSocketAddress address;
    private WritableWrapper writable;
    /** Max time in millisecond to wait for a return */
    private long timeout;
    /** Hold a Listener thread waiting for reply */
    private Listener listener;
    
    /** Static call ID counter */
    private static AtomicInteger counter = new AtomicInteger();
    /** Static socket channel pool */
    private static SocketChannelPool channelPool = new SocketChannelPool(true);
    
    private class Listener extends Thread {
        
        public Listener() throws IOException {
            selector = Selector.open();
        }

        public void register(SocketChannel channel, int ops, Object att) {
            pending.incrementAndGet();
            selector.wakeup();
            try {
                channel.register(selector, ops, att);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            } finally {
                int cnt = pending.decrementAndGet();
                if (cnt <= 0) synchronized (pending) {
                    pending.notify();
                }
            }
        }
        
        private void waitPending() throws InterruptedException {
            synchronized (pending) {
                while (pending.get() > 0) {
                    pending.wait();
                }
            }
        }
        
        @Override
        public void run() {
            if (Log.DEBUG)
                Log.info("Client listener starts.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    waitPending(); // handle new registrations
                    if (selector.select() > 0) {
                        Set<SelectionKey> selectedKeys = 
                            selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (Log.DEBUG) Log.state(1000, "Selecting keys ...");
                            if (key.isValid() && key.isReadable()) {
                                InputChannelBuffer inBuf = (InputChannelBuffer) key.attachment();
                                if (inBuf != null && doRead(inBuf)) {
                                    key.cancel();
                                    channelPool.putSocketChannel(inBuf.getChannel());
                                }
                            }
                        } // for
                        selectedKeys.clear();
                    } else Thread.yield();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // Release occupied connection resources
            channelPool.clear();
            Log.info("Client listener shuts down (.connection pool size)",
                    channelPool.size());
        }
        
        /** Read complete remote call replyQueue 
         * @return true if RPC's replies are ready or fail, 
         *     false if further reading is necessary
         */
        private boolean doRead(InputChannelBuffer inBuf) {
            try {
                byte[] data = inBuf.read();
                if (data != null) {
                    if (Log.DEBUG) {
                        Log.action("Receive reply with .length", data.length);
                    }
                    DataInputStream dataIn = new DataInputStream(
                            new ByteArrayInputStream(data));
                    // Read in call ID
                    Writable callID = writable.newInstance(Integer.class);
                    callID.readFields(dataIn);
                    // Pass on data to corresponding result set
                    ReplySet results = ReplySet.get((Integer)callID.getValue());
                    if (results != null) {
                        Writable error = writable.newInstance(Boolean.class);
                        error.readFields(dataIn);
                        if ((Boolean)error.getValue()) {
                            if (dataIn.available() == 0) {
                                // end of frame denoting final return
                                results.putEnd();
                                return true;
                            } else {
                                Writable errorMessage = writable.newInstance(String.class);
                                errorMessage.readFields(dataIn);
                                results.putError(errorMessage.getValue().toString());
                                return false;
                            }
                        } else return results.putData(dataIn);
                    } else if (Log.DEBUG) { // reply set is null
                        Log.info("No such reply set #:", callID.getValue());
                    }
                } else { // data == null
                    // There does exist null reading in, which is normal
                    return false;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        } // doRead
        
        /** Hold a selector waiting for reply */
        private Selector selector;
        private AtomicInteger pending = new AtomicInteger();
    } // Listener
}
