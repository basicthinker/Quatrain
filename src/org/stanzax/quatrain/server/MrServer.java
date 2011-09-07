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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.stanzax.quatrain.io.ByteArrayOutputStream;
import org.stanzax.quatrain.io.InputChannelBuffer;
import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.OutputChannelBuffer;
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
        bindAddress = new InetSocketAddress(address, port);
        
        listener = new Listener();
        listener.setDaemon(false);
        
        registrant = new Registrant();
        registrant.setDaemon(false);
        
        responder = new Responder();
        responder.setDaemon(false);
        
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
        registrant.start();
        responder.start();
        Log.info("Quatrain Service Starts. This is an event-based server.");
    }

    public void stop() {
        isRunning = false;
    }

    protected void preturn(Object value) {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        
        ByteBuffer reply = packData(callID, false, value);
        registrant.put(channel, reply, false);
        
        if (Log.DEBUG) Log.action(
                "Queued reply for .callID with .length", 
                (int)callID, reply.capacity());
    }

    private void freturn() {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        
        // Final break according to the ordering protocol
        AtomicInteger order = orders.get(callID);
        while (order.get() != 0)
            Thread.yield();
        
        ByteBuffer reply = packData(callID, false, new EOR());
        registrant.put(channel, reply, true);

        orders.remove(callID);
        if (Log.DEBUG) Log.action(
                "Queued reply for .callID with .length and removed order", 
                (int)callID, reply.capacity());
    }
    
    /**
     * Construct reply main body (call ID + error flag + value)
     * */
    private ByteBuffer packData(long callID, boolean error, Object value) {
        ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(1024);
        DataOutputStream dataOut = new DataOutputStream(arrayOut);
        try {
            dataOut.writeInt(0); // occupied ahead for length
            writable.valueOf((int)callID).write(dataOut); //cast long call ID to original integer type
            writable.valueOf(error).write(dataOut);
            writable.valueOf(value).write(dataOut);
            dataOut.flush();
        } catch (IOException e) {
            System.err.println("@MrServer.packData: " + e.getMessage());
            return null;
        }
        // Allocate byte buffer and insert ahead data length
        int dataLength = dataOut.size();
        ByteBuffer replyBuffer = ByteBuffer.wrap(arrayOut.getByteArray(), 
                0, dataLength);
        replyBuffer.putInt(0, dataLength - 4);
        return replyBuffer;
    }
    
    private InetSocketAddress bindAddress;
    /** Each server holds one thread listening to target socket address */
    private Listener listener;
    private Registrant registrant;
    private Responder responder;
    
    /** Denote whether this server is still running */
    private volatile boolean isRunning;
    /** Writable factory to produce proper type of instance */
    private WritableWrapper writable;
    /** Thread pool executor to execute Handlers */
    private ThreadPoolExecutor handlerExecutor;
    
    /** Set by Handler and retrieved by preturn() to get the connection */
    private final InheritableThreadLocal<SocketChannel> threadChannel = 
        new InheritableThreadLocal<SocketChannel>();
    /** Set by Handler and retrieved by preturn() to construct reply header */
    private final InheritableThreadLocal<Long> threadCallID = 
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
    
    private class Listener extends java.lang.Thread {

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
            } catch (IOException e) {
                System.err.println("@MrServer.Listener.doAccept: " + e.getMessage());
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
                    System.err.println("@MrServer.Listener.doRead: " + e.getMessage());
                }
            }
        }

        /** Private selector to tell socket events */
        private Selector selector;
    } // Listener

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
            Writable rawCallID = writable.newInstance(Integer.class);
            try {
                rawCallID.readFields(dataIn);
            } catch (IOException e) {
                System.err.println("@MrServer.Handler.run: while reading in call ID: " + e.getMessage());
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
                
                if (Log.DEBUG) {
                    StringBuilder strParameters = new StringBuilder();
                    for (Object p : parameters) {
                        strParameters.append("{").append(p).append("}");
                    }
                    Log.action("Invoking procedure", procedureName.getValue(), strParameters);
                }
                
                procedure.invoke(MrServer.this, parameters);
                
            } catch (Exception e) {
                System.err.println("@MrServer.Handler.run: while reading in and invoking: " + e.getMessage());
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
    
    private class Registrant extends java.lang.Thread {   
        
        public void run() {
            while (isRunning) {
                try {
                    ReplyData reply = queue.take();
                    responder.register(reply.channel, reply.data, reply.isFinal);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        public void put(SocketChannel channel, ByteBuffer data, boolean isFinal) {
            ReplyData reply = new ReplyData(channel, data, isFinal);
            try {
                queue.put(reply);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        private BlockingQueue<ReplyData> queue = 
            new LinkedBlockingQueue<ReplyData>();
    } // Registrant
    
    private class Responder extends java.lang.Thread {

        public Responder() throws IOException {
            selector = Selector.open();
        }
        
        /**
         * Register the socket channel with the writing selector if it has not been registered,
         * otherwise append the reply data to the registered key's attachment (OutputChannelBuffer).
         * Notice: this method is NOT thread-safe, and should only be invoked within a single thread.
         */
        public void register(SocketChannel channel, ByteBuffer data, boolean isFinal) {
            SelectionKey key = channel.keyFor(selector);
            if (key != null) {
                OutputChannelBuffer out = (OutputChannelBuffer) key.attachment();
                out.putData(data, isFinal);
            } else { // try to make new register of channel
                synchronized (this) {
                    selector.wakeup();
                    try {
                        channel.register(selector, SelectionKey.OP_WRITE, 
                                new OutputChannelBuffer(channel, data, isFinal));
                    } catch (ClosedChannelException e) {
                        System.err.println("@MrServer.Responder.register: " + e.getMessage());
                    }
                }
            }
        }
        
        @Override
        public void run() {
            while (isRunning){
                try {
                    synchronized (this) {}
                    if (selector.select() > 0) {
                        Set<SelectionKey> selectedKeys = selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (key.isValid() && key.isWritable()) {
                                OutputChannelBuffer outBuf = 
                                    (OutputChannelBuffer)key.attachment();
                                if (outBuf != null && !outBuf.write()) {
                                    key.cancel();
                                }
                            }
                        }
                        selectedKeys.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }   
            }
        }
        
        /** Private selector to tell socket events */
        private Selector selector;
        
    } // Responder
}
