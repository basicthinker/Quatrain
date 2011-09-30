/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.stanzax.quatrain.io.ByteArrayOutputStream;
import org.stanzax.quatrain.io.DirectWritable;
import org.stanzax.quatrain.io.InputChannelBuffer;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.SocketChannelPool;

/**
 * @author basicthinker
 * 
 */
public class MrClient {

    public MrClient(InetAddress host, int port, long timeout, Configuration conf) 
    throws IOException {
        this(new InetSocketAddress(host, port), timeout);
    }
    
    public MrClient(InetSocketAddress address, long timeout) 
    throws IOException {
        this.address = address;
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
    
    public ReplySet invoke(Class<? extends Writable> returnType, String procedureName) {
        return invoke(timeout, new ReplySet.Internal(returnType, timeout, conf), 
                procedureName, new Writable[0]);
    }
    
    public ReplySet invoke(Class<? extends Writable> returnType, String procedureName,
            Writable...parameters) {
        return invoke(timeout, new ReplySet.Internal(returnType, timeout, conf),
                procedureName, parameters);
    }
    
    public ReplySet invoke(DirectWritable writable, 
            String procedureName) throws Exception {
        return invoke(timeout, new ReplySet.External(writable, timeout), 
                procedureName, new Writable[0]);
    }
    
    public ReplySet invoke(DirectWritable writable, 
            String procedureName, Writable...parameters) throws Exception {
        return invoke(timeout, new ReplySet.External(writable, timeout),
                procedureName, parameters);
    }
    
    private ReplySet invoke(long timeout, ReplySet results, String procedureName,
            Writable...parameters) {
        int callID = counter.incrementAndGet();
        // Early create and register result set for awaiting reply
        results.register(callID);
        SocketChannel channel = null;
        try {
            channel = channelPool.getSocketChannel(address);
            ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(128);
            DataOutputStream dataOut = new DataOutputStream(arrayOut);
            dataOut.writeInt(0); // occupied ahead for length
            new IntWritable(callID).write(dataOut); //write call ID
            new Text(procedureName).write(dataOut); //write the procedure name
            for (Writable parameter : parameters) { //write procedure parameters
                parameter.write(dataOut);
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
        } catch (Exception e) {
            System.err.println("@MrClient.invoke: " +  e.getMessage());
            try {
                channel.socket().close();
            } catch (Exception ex) { } 
            try {
                channel.close();
            } catch (Exception ex) { }
            channel = null;
        }
        if (channel != null) {
            listener.register(channel, SelectionKey.OP_READ, 
                    new InputChannelBuffer(channel));
        }
        return results;
    }

    /** Hold a socket address of the target server */
    private InetSocketAddress address;
    /** Max time in millisecond to wait for a return */
    private long timeout;
    /** Hold a Listener thread waiting for reply */
    private Listener listener;
    private Configuration conf;
    
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
                if (channel.isOpen()) {
                    channel.register(selector, ops, att);
                }
            } catch (Exception e) {
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
                    selector.select();
                    SocketChannel channel;
                    while ((channel = toRegisterRead.poll()) != null) {
                        register(channel, SelectionKey.OP_READ, 
                                new InputChannelBuffer(channel));
                    }
                    Set<SelectionKey> selectedKeys = 
                        selector.selectedKeys();
                    for (SelectionKey key : selectedKeys) {
                        if (Log.DEBUG) Log.state(1000, "Selecting keys ...");
                        if (key.isValid() && key.isReadable()) {
                            InputChannelBuffer inBuf = (InputChannelBuffer) key.attachment();
                            if (inBuf != null && doRead(key)) {
                                channelPool.putSocketChannel(inBuf.getChannel());
                            }
                        }
                    } // for
                    selectedKeys.clear();
                } catch (Exception e) {
                    System.err.println("@MrClient.Listener.run: " +  e.getMessage());
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
        private boolean doRead(SelectionKey key) {
            InputChannelBuffer inBuf = (InputChannelBuffer)key.attachment();
            try {
                byte[] data = inBuf.read();
                if (data != null) {
                    if (Log.DEBUG) {
                        Log.action("Receive reply with .length", data.length);
                    }
                    DataInputStream dataIn = new DataInputStream(
                            new ByteArrayInputStream(data));
                    // Read in call ID
                    IntWritable callID = new IntWritable();
                    callID.readFields(dataIn);
                    // Pass on data to corresponding result set
                    final ReplySet results = ReplySet.get(callID.get());
                    if (results != null) {
                        byte type = dataIn.readByte();
                        switch (type) {
                        case ReplySet.INTERNAL:
                            if (results.putData(dataIn)) {
                                return false;
                            }
                            break;
                        case ReplySet.EXTERNAL:
                            final SocketChannel channel = inBuf.getChannel();
                            key.cancel();
                            channel.configureBlocking(true);
                            new Thread(new Runnable() {

                                @Override
                                public void run() {
                                    if (results.putData(channel)) {
                                        try {
                                            channel.configureBlocking(false);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        toRegisterRead.add(channel);
                                        selector.wakeup();
                                    }
                                }
                                
                            }).start();
                            return false;
                        case ReplySet.ERROR:
                            if (dataIn.available() == 0) {
                                // end of frame denoting final return
                                results.putEnd();
                            } else {
                                Text errorMessage = new Text();
                                errorMessage.readFields(dataIn);
                                results.putError(errorMessage.toString());
                                return false;
                            }
                            break;
                        default:
                            System.err.println("@MrClient.Listener.doRead: Wrong reply type byte " + type);
                        }
                    } else if (Log.DEBUG) { // reply set is null
                        Log.info("No such reply set #:", callID.get());
                    }
                } else { // data == null
                    // There does exist null reading in, which is normal
                    return false;
                }
            } catch (Exception e) {
                System.err.println("@MrClient.Listener.doRead: " + e.toString() +
                        " : " + e.getMessage());
            }
            key.cancel();
            return true;
        } // doRead
        
        /** Hold a selector waiting for reply */
        private Selector selector;
        private AtomicInteger pending = new AtomicInteger();
        private ConcurrentLinkedQueue<SocketChannel> toRegisterRead = 
                new ConcurrentLinkedQueue<SocketChannel>(); 
    } // Listener
}
