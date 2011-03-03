/**
 * 
 */
package org.stanzax.quatrain.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.stanzax.quatrain.hadoop.BooleanWritable;
import org.stanzax.quatrain.hadoop.IntWritable;
import org.stanzax.quatrain.hadoop.LongWritable;
import org.stanzax.quatrain.io.ChannelBuffer;
import org.stanzax.quatrain.io.DataOutputBuffer;
import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 * 
 */
public class MrServer {

    /** User can configure server by providing refined thread pool executors. */
    public MrServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor,
            ThreadPoolExecutor responderExecutor) throws IOException {
        this.bindAddress = new InetSocketAddress(address, port); // set up bind
        // address
        this.threadChannel = new InheritableThreadLocal<SocketChannel>();
        this.threadCallID = new InheritableThreadLocal<Long>();

        this.listener = new java.lang.Thread(new Listener()); // create listener thread
        this.listener.setDaemon(false);

        this.handlerExecutor = handlerExecutor;
        this.responderExecutor = responderExecutor;
        this.writable = wrapper;
    }

    public void start() {
        isRunning = true;
        listener.start();
        Log.info("Quatrain Service Starts.");
    }

    public void stop() {
        isRunning = false;
    }

    protected void preturn(Object value) {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        // Second-level primitive according to the two-level ordering protocol
        orders.get(callID).second.incrementAndGet(); // locate between first-level primitives
        Responder responder = new Responder(channel, callID, false,
                value);
        responderExecutor.execute(responder);
    }

    private void freturn() {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        
        // Order according to the two-level ordering protocol
        Order order = orders.get(callID);
        while (order.first.get() != 0 || order.second.get() != 0);
        
        Responder responder = new Responder(channel, callID, false,
                new EOR());
        responderExecutor.execute(responder);
        
        // Final break according to the two-level ordering protocol
        while (order.second.get() == 0);
        orders.remove(callID);
    }
    
    InetSocketAddress bindAddress;

    /** Each server holds one thread listening to target socket address */
    protected java.lang.Thread listener;
    /** Denote whether this server is still running */
    protected volatile boolean isRunning;
    /** Writable factory to produce proper type of instance */
    protected WritableWrapper writable;
    /** Thread pool executor to execute Handlers */
    protected ThreadPoolExecutor handlerExecutor;
    /** Thread pool executor to execute Responders */
    protected ThreadPoolExecutor responderExecutor;
    /** Set by Handler and retrieved by preturn() to get the connection */
    protected final InheritableThreadLocal<SocketChannel> threadChannel;
    /** Set by Handler and retrieved by preturn() to construct reply header */
    protected final InheritableThreadLocal<Long> threadCallID;
    /**
     * Save thread running states for each request 
     * according to the two-level ordering protocol
     */
    protected ConcurrentHashMap<Long, Order> orders = 
        new ConcurrentHashMap<Long, Order>();

    protected class Thread extends java.lang.Thread {

        public Thread() {
            super();
        }

        public Thread(Runnable target) {
            super(target);
        }

        public Thread(String name) {
            super(name);
        }
        
        public void start() {
            // according to two-level ordering protocal
            orders.get(threadCallID.get()).first.incrementAndGet(); // before zero-level freturn()

            super.start();
            
            // according to two-level ordering protocal
            orders.get(threadCallID.get()).first.decrementAndGet(); // shrink after preturn() called
        }

    }

    private class Listener implements Runnable {

        public Listener() throws IOException {
            // Initialize one server-socket channel for acceptance
            ServerSocketChannel acceptChannel = ServerSocketChannel.open();
            acceptChannel.socket().bind(bindAddress);
            acceptChannel.configureBlocking(false);
            // Create the multiplexor of channels
            selector = Selector.open();
            // Register initial acceptance channel to selector
            // so that the selector monitors the channel's acceptance events
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    if (selector.select() > 0) { // if there exist new events
                        Set<SelectionKey> selectedKeys = 
                            selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (key.isAcceptable()) {
                                // retrieve the associated acceptance channel
                                ServerSocketChannel acceptChannel = 
                                    (ServerSocketChannel) key.channel();
                                // create connection and register reading
                                // channel
                                SocketChannel readChannel = 
                                    acceptChannel.accept();
                                if (readChannel != null) {
                                    readChannel.configureBlocking(false);
                                    readChannel.socket().setTcpNoDelay(true);
                                    SelectionKey readKey = readChannel.register(
                                            selector, SelectionKey.OP_READ);
                                    readKey.attach(new ChannelBuffer(readChannel));
                                }
                            } else if (key.isReadable()) {
                                ChannelBuffer channel = 
                                    (ChannelBuffer) key.attachment();
                                readAndProcess(channel);
                            }
                        }
                        selectedKeys.clear();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /** Read complete remote call requests */
        private void readAndProcess(ChannelBuffer channelBuffer) throws IOException {
            if (channelBuffer.hasLength() || channelBuffer.tryReadLength()) {
                if (channelBuffer.tryReadData()) {
                    if (Log.debug) Log.debug(
                            "Read data of .length", channelBuffer.getLength());
                    // Create and trigger handler
                    Handler handler = new Handler(
                            channelBuffer.getData(), channelBuffer.getChannel());
                    handlerExecutor.execute(handler);
                } 
            } 
        }

        /** Hold one selector to tell socket events */
        private Selector selector;
    }

    private class Handler implements Runnable {

        public Handler(byte[] data, SocketChannel channel) {
            this.data = data;
            this.channel = channel;
        }

        @Override
        public void run() {
            DataInputStream dataIn = new DataInputStream(
                    new ByteArrayInputStream(data));
            // Read in integer call ID
            IntWritable rawCallID = new IntWritable();
            try {
                rawCallID.readFields(dataIn);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // Transfer original call ID to inner long presentation
            long callID = channel.hashCode();
            callID = (callID << 32) + rawCallID.get();
            // Set thread locals before creating method threads
            threadCallID.set(callID);
            threadChannel.set(channel);
            
            orders.put(callID, new Order());
            // First-level primitive according to two-level ordering protocal
            orders.get(callID).first.incrementAndGet(); // before ending freturn()
            
            // TODO Invoke corresponding procedure
            IntWritable parameter = new IntWritable();
            try {
                parameter.readFields(dataIn);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] returnValue = new String[2];
            returnValue[0] = "1st";
            returnValue[1] = "2nd";
            // TODO Reply to this call in preturn()
            preturn(1);
            
            // First-level primitive according to two-level ordering protocal
            orders.get(callID).first.decrementAndGet(); // shrink after thread creation
            
            freturn(); // final return
        }

        private byte[] data;
        private SocketChannel channel;
    }

    private class Responder implements Runnable {

        public Responder(SocketChannel channel, long callID, 
                boolean error, Object value) {
            this.channel = channel;
            this.callID = callID;
            this.error = error;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                // Construct reply main body (call ID + error flag + value)
                DataOutputBuffer dataOut = new DataOutputBuffer();
                // cast inner long call ID to original integer type
                new IntWritable((int)callID).write(dataOut);
                new BooleanWritable(error).write(dataOut);
                writable.valueOf(value).write(dataOut);
                dataOut.flush();
                // Add data length
                int dataLength = dataOut.getDataLength();
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4).putInt(dataLength);
                lengthBuffer.flip();
                
                synchronized (channel) {
                    channel.write(lengthBuffer);
                    channel.write(ByteBuffer.wrap(dataOut.getData(),
                            0, dataLength));
                }
                
                // Second-level primitive according to two-level ordering protocol
                orders.get(callID).second.decrementAndGet(); // shrink after data transmission
                
                if (Log.debug) Log.debug("Reply to .callID .length", (int)callID, dataLength);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private SocketChannel channel;
        private long callID;
        private boolean error;
        private Object value;
    }
}
