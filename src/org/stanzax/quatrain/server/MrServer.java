/**
 * 
 */
package org.stanzax.quatrain.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.stanzax.quatrain.io.ByteArrayOutputStream;
import org.stanzax.quatrain.io.InputChannelBuffer;
import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 * 
 */
public class MrServer {

    public MrServer(String address, int port, WritableWrapper wrapper,
            int handlerCount) throws IOException {
        this(address, port, wrapper, new ThreadPoolExecutor(
                handlerCount, 2 * handlerCount, 6, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()));
    }
    
    /** User can configure server by providing refined thread pool executors. */
    public MrServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor) throws IOException {
        this.bindAddress = new InetSocketAddress(address, port);

        this.listener = new java.lang.Thread(new Listener());
        this.listener.setDaemon(false);
        
        this.handlerExecutor = handlerExecutor;
        this.writable = wrapper;
    }

    public void start() {
        // Store all user-defined methods in hash map for quick retrieval
        Method[] publicMethods = this.getClass().getMethods();
        for (Method method : publicMethods) {
            procedures.put(method.getName(), method);
        }
        // Remove all system procedures for convenience as well as security
        procedures.remove("main");
        procedures.remove("start");
        procedures.remove("stop");
        procedures.remove("wait");
        procedures.remove("equals");
        procedures.remove("toString");
        procedures.remove("hashCode");
        procedures.remove("getClass");
        procedures.remove("notify");
        procedures.remove("notifyAll");
        // Print all registered methods for adminstrator to verify
        System.out.println("All registered procedures:");
        Set<String> procedureNames = procedures.keySet();
        for (String name : procedureNames) {
            System.out.println(" - " + name);
        }
        System.out.println("Please garantee no extra methods exit for security concern.");
        
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
        
        respond(channel, callID, (byte) 0, value);
    }

    private void freturn() {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        
        // Final break according to the ordering protocol
        AtomicInteger order = orders.get(callID);
        while (order.get() != 0)
            Thread.yield();
        
        try {
            respond(channel, callID, (byte) -1, new EOR());
        } finally {
            orders.remove(callID);
            if (Log.DEBUG) Log.action("Order removed for", callID);
        }

        try {
            channel.socket().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void respond(SocketChannel channel,
            long callID, byte type, Object value) {
        try {
            if (Log.DEBUG) Log.state(1, "Responder is running ...", 1);
            // Prepare dynamic composition of reply
            ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(1024);
            DataOutputStream dataOut = new DataOutputStream(arrayOut);
            // Compose reply header
            dataOut.writeInt(0); // occupied ahead for length
            dataOut.writeInt((int)callID); //cast long call ID to original int
            dataOut.writeByte(type);
            // Compose main body
            writable.valueOf(value).write(dataOut);
            dataOut.flush();
            // Allocate byte buffer and insert ahead data length
            int dataLength = dataOut.size();
            ByteBuffer replyBuffer = ByteBuffer.wrap(arrayOut.getByteArray(), 
                    0, dataLength);
            replyBuffer.putInt(0, dataLength - 4);
            
            assert(!channel.isBlocking());
            synchronized (channel) {
                channel.write(replyBuffer);
                while (replyBuffer.hasRemaining()) {
                    Thread.yield();
                    channel.write(replyBuffer);
                }
            }
            
            if (Log.DEBUG) Log.action("Reply to .callID .length", callID, dataLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private InetSocketAddress bindAddress;

    /** Each server holds one thread listening to target socket address */
    private java.lang.Thread listener;
    
    /** Denote whether this server is still running */
    private volatile boolean isRunning;
    /** Writable factory to produce proper type of instance */
    private WritableWrapper writable;
    /** Thread pool executor to execute Handlers */
    private ThreadPoolExecutor handlerExecutor;
    
    /** Set by Handler and retrieved by preturn() to get the connection */
    protected final InheritableThreadLocal<SocketChannel> threadChannel = 
        new InheritableThreadLocal<SocketChannel>();
    /** Set by Handler and retrieved by preturn() to construct reply header */
    protected final InheritableThreadLocal<Long> threadCallID = 
        new InheritableThreadLocal<Long>();
    
    private Random random = new Random();
    /**
     * Save thread running states for each request 
     * according to the ordering protocol
     */
    private ConcurrentHashMap<Long, AtomicInteger> orders = 
        new ConcurrentHashMap<Long, AtomicInteger>();
    /** Method hash map */
    private HashMap<String, Method> procedures =
        new HashMap<String, Method>();

    protected class Thread extends java.lang.Thread {

        public Thread(Runnable target) {
            super(new OrderRunnable(target));
        }
        
        public void start() {
            // according to ordering protocol
            orders.get(threadCallID.get()).incrementAndGet(); // before zero-level freturn()
            if (Log.DEBUG) Log.action("Thread # [+] sub order", Thread.currentThread().getId());
            super.start();
        }

        public final void run() {
            super.run();
        }
    }

    private class OrderRunnable implements Runnable {
        public OrderRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } finally {
                // according to ordering protocol
                orders.get(threadCallID.get()).decrementAndGet(); // shrink after preturn()
                if (Log.DEBUG) Log.action("Thread # [-] sub order", 
                        Thread.currentThread().getId());
            }
        }
        
        private Runnable runnable;

    }
    
    private class Listener implements Runnable {

        public Listener() throws IOException {
            // Initialize one server-socket channel for acceptance
            ServerSocketChannel acceptChannel = ServerSocketChannel.open();
            acceptChannel.socket().bind(bindAddress);
            // Create the multiplexor of channels
            selector = Selector.open();
            // Register initial acceptance channel to selector
            // so that the selector monitors the channel's acceptance events
            acceptChannel.configureBlocking(false);
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    if (Log.DEBUG) Log.state(1, "Listener is running ...", 1);
                    if (selector.select() > 0) { // if there exist new events
                        Set<SelectionKey> selectedKeys = 
                            selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (Log.DEBUG) Log.state(1, "Select keys ...");
                            if (key.isValid()) {
                                if (key.isAcceptable()) {
                                    doAccept(key);
                                } else if (key.isReadable()) {
                                    doRead(key);
                                } 
                            }
                        } // for
                        selectedKeys.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void doAccept(SelectionKey key) {
            try {
                // retrieve the associated acceptance channel
                ServerSocketChannel acceptChannel = 
                    (ServerSocketChannel) key.channel();
                // establish connection and reading channel
                SocketChannel readChannel = 
                    acceptChannel.accept();
                if (readChannel != null) {
                    readChannel.configureBlocking(false);
                    readChannel.socket().setTcpNoDelay(true);
                    SelectionKey readKey = readChannel.register(
                            selector, SelectionKey.OP_READ);
                    readKey.attach(new InputChannelBuffer(readChannel));
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        /** Read complete remote call requests */
        private void doRead(SelectionKey key) {
            InputChannelBuffer inBuf = (InputChannelBuffer) key.attachment();
            if (inBuf != null) {
                try {
                    byte[] data = inBuf.read();
                    if (data != null) { // after reading in the whole frame
                        if (Log.DEBUG) Log.action(
                                "Read data of .length", data.length);
                        key.cancel();
                        // Create and trigger handler
                        Handler handler = new Handler(
                                data, inBuf.getChannel());
                        handlerExecutor.execute(handler); 
                    } 
                } catch (IOException e) {
                    key.cancel();
                    e.printStackTrace();
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
            if (Log.DEBUG) Log.action("Handler starts ...");
            DataInputStream dataIn = new DataInputStream(
                    new ByteArrayInputStream(data));
            
            // Read in integer call ID
            Writable rawCallID = writable.newInstance(Integer.class);
            try {
                rawCallID.readFields(dataIn);
            } catch (IOException ex) {
                ex.printStackTrace();
                return;
            }
            // Transfer original call ID to inner long type
            long callID = random.nextInt();
            callID = (callID << 32) + (Integer)rawCallID.getValue();
            // Set thread locals before creating method threads
            threadCallID.set(callID);
            threadChannel.set(channel);
            
            // Create order for the ordering protocol
            AtomicInteger order = new AtomicInteger(1);
            orders.put(callID, order);
            if (Log.DEBUG) Log.action("Thread # [+] 1st-color order", Thread.currentThread().getId());
            
            try {
                // Invoke corresponding procedure
                Writable procedureName = writable.newInstance(String.class);
                procedureName.readFields(dataIn);
                
                Method procedure = procedures.get(procedureName.getValue());
                Class<?>[] parameterTypes = procedure.getParameterTypes();
                int parameterCount = parameterTypes.length;
                Object[] parameters = new Object[parameterCount];
                for (int i = 0; i < parameterCount; ++i) {
                    Writable writableParameter = writable.newInstance(parameterTypes[i]);
                    writableParameter.readFields(dataIn);
                    parameters[i] = writableParameter.getValue();
                }
                procedure.invoke(MrServer.this, parameters);
                if (Log.DEBUG) {
                    StringBuilder strParameters = new StringBuilder();
                    for (Object p : parameters) {
                        strParameters.append("{").append(p).append("}");
                    }
                    Log.action("Invoked procedure", procedureName.getValue(), strParameters);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Primitive according to ordering protocal
                orders.get(callID).decrementAndGet(); // shrink after thread creation
                if (Log.DEBUG) Log.action("Thread # [-] 1st-color order", Thread.currentThread().getId());
            }
            freturn(); // final return
        }

        private byte[] data;
        private SocketChannel channel;
    } // Handler
}
