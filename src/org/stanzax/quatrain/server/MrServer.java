/**
 * 
 */
package org.stanzax.quatrain.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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

import org.stanzax.quatrain.io.ChannelBuffer;
import org.stanzax.quatrain.io.DataOutputBuffer;
import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 * 
 */
public class MrServer {

    public MrServer(String address, int port, WritableWrapper wrapper, int handlerCount,
            int responderCount) throws IOException {
        this(address, port, wrapper, new ThreadPoolExecutor(
                handlerCount, 2 * handlerCount, 6, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()), new ThreadPoolExecutor(
                responderCount, 2 * responderCount, 6, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()));
    }
    
    /** User can configure server by providing refined thread pool executors. */
    public MrServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor,
            ThreadPoolExecutor responderExecutor) throws IOException {
        this.bindAddress = new InetSocketAddress(address, port);
        this.threadChannel = new InheritableThreadLocal<SocketChannel>();
        this.threadCallID = new InheritableThreadLocal<Long>();

        this.listener = new java.lang.Thread(new Listener()); // create listener thread
        this.listener.setDaemon(false);

        this.handlerExecutor = handlerExecutor;
        this.responderExecutor = responderExecutor;
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
        // Second-level primitive according to the two-level ordering protocol
        orders.get(callID).second.incrementAndGet(); // locate between first-level primitives
        if (Log.DEBUG) Log.action("Thread .ID [+] 2nd-level order", Thread.currentThread().getId());
        
        Responder responder = new Responder(channel, callID, false,
                value);
        responderExecutor.execute(responder);
    }

    private void freturn() {
        SocketChannel channel = threadChannel.get();
        long callID = threadCallID.get();
        
        // Order according to the two-level ordering protocol
        Order order = orders.get(callID);
        while (order.first.get() != 0 || order.second.get() != 0)
            Thread.yield();
        
        Responder responder = new Responder(channel, callID, false,
                new EOR());
        responderExecutor.execute(responder);
        
        // Final break according to the two-level ordering protocol
        while (order.second.get() >= 0)
            Thread.yield();
        
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (Log.DEBUG) Log.action("Order removed for", callID);
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
    protected Random random = new Random();
    /** Set by Handler and retrieved by preturn() to construct reply header */
    protected final InheritableThreadLocal<Long> threadCallID;
    /**
     * Save thread running states for each request 
     * according to the two-level ordering protocol
     */
    protected ConcurrentHashMap<Long, Order> orders = 
        new ConcurrentHashMap<Long, Order>();
    /** Method hash map */
    protected HashMap<String, Method> procedures =
        new HashMap<String, Method>();

    protected class Thread extends java.lang.Thread {

        public Thread(Runnable target) {
            super(new OrderRunnable(target));
        }
        
        public void start() {
            // according to two-level ordering protocol
            orders.get(threadCallID.get()).first.incrementAndGet(); // before zero-level freturn()
            if (Log.DEBUG) Log.action("Thread .ID [+] 1st-level order", Thread.currentThread().getId());
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
            runnable.run();
            // according to two-level ordering protocol
            orders.get(threadCallID.get()).first.decrementAndGet(); // shrink after preturn() called
            if (Log.DEBUG) Log.action("Thread .ID [-] 1st-level order", Thread.currentThread().getId());
        }
        
        private Runnable runnable;

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
                    if (Log.DEBUG) Log.state(1, "Listener is running ...", 1);
                    if (selector.select() > 0) { // if there exist new events
                        Set<SelectionKey> selectedKeys = 
                            selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (Log.DEBUG) Log.state(1, "Select keys ...");
                            if (key.isAcceptable()) {
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
                                    readKey.attach(new ChannelBuffer(readChannel));
                                }
                            } else if (key.isReadable()) {
                                readAndProcess(key);
                            }
                        }
                        selectedKeys.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /** Read complete remote call requests */
        private void readAndProcess(SelectionKey key) throws IOException {
            ChannelBuffer channelBuffer = (ChannelBuffer) key.attachment();
            if (channelBuffer != null) {
                byte[] data = channelBuffer.read();
                if (data != null) { // after reading in the whole frame
                    if (Log.DEBUG) Log.action(
                            "Read data of .length", data.length);
                    key.cancel();
                    // Create and trigger handler
                    Handler handler = new Handler(
                            data, channelBuffer.getChannel());
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
            if (Log.DEBUG) Log.state(1, "Handler is running ...", 1);
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
                
            // Create order for the two-level ordering protocol 
            orders.put(callID, new Order());
            // First-level primitive according to two-level ordering protocol
            orders.get(callID).first.incrementAndGet(); // before ending freturn()
            if (Log.DEBUG) Log.action("Thread .ID [+] 1st-level order", Thread.currentThread().getId());
            
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
                    StringBuffer strParameters = new StringBuffer();
                    for (Object p : parameters) {
                        strParameters.append("{").append(p).append("}");
                    }
                    Log.action("Invoked procedure", procedureName.getValue(), strParameters);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // First-level primitive according to two-level ordering protocal
                orders.get(callID).first.decrementAndGet(); // shrink after thread creation
                if (Log.DEBUG) Log.action("Thread .ID [-] 1st-level order", Thread.currentThread().getId());
            }
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
                if (Log.DEBUG) Log.state(1, "Responder is running ...", 1);
                // Construct reply main body (call ID + error flag + value)
                DataOutputBuffer dataOut = new DataOutputBuffer();
                writable.valueOf((int)callID).write(dataOut); //cast long call ID to original integer type
                writable.valueOf(error).write(dataOut);
                writable.valueOf(value).write(dataOut);
                dataOut.flush();
                // Calculate data length
                int dataLength = dataOut.getDataLength();
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4).putInt(dataLength);
                lengthBuffer.flip();
                
                synchronized (channel) {
                    channel.write(lengthBuffer);
                    channel.write(ByteBuffer.wrap(dataOut.getData(),
                            0, dataLength));
                }                
                if (Log.DEBUG) Log.action("Reply to .callID .length", callID, dataLength);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                // Second-level primitive according to two-level ordering protocol
                orders.get(callID).second.decrementAndGet(); // shrink after data transmission
            }
        }

        private SocketChannel channel;
        private long callID;
        private boolean error;
        private Object value;
    }
}
