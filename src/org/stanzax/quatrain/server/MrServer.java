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
import java.util.concurrent.ThreadPoolExecutor;

import org.stanzax.quatrain.hadoop.IntWritable;
import org.stanzax.quatrain.io.ChannelBuffer;
import org.stanzax.quatrain.io.DataOutputBuffer;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;
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
        this.channel = new InheritableThreadLocal<SocketChannel>();
        this.callID = new InheritableThreadLocal<Long>();

        this.listener = new Thread(new Listener()); // create listener thread
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
        // TODO Method stub
        long id = callID.get();
    }

    InetSocketAddress bindAddress;

    /** Each server holds one thread listening to target socket address */
    protected Thread listener;
    /** Denote whether this server is still running */
    protected volatile boolean isRunning;
    /** Writable factory to produce proper type of instance */
    protected WritableWrapper writable;
    /** Thread pool executor to execute Handlers */
    protected ThreadPoolExecutor handlerExecutor;
    /** Thread pool executor to execute Responders */
    protected ThreadPoolExecutor responderExecutor;
    /** Set by Handler and retrieved by preturn() to get the connection */
    protected final InheritableThreadLocal<SocketChannel> channel;
    /** Set by Handler and retrieved by preturn() to construct reply header */
    protected final InheritableThreadLocal<Long> callID;

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
            // Read in call ID
            Writable callID = writable.newInstance(Integer.TYPE);
            try {
                callID.readFields(dataIn);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // TODO Invoke corresponding method
            IntWritable parameter = 
                (IntWritable) writable.newInstance(Integer.TYPE);
            try {
                parameter.readFields(dataIn);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // TODO Reply to this call in preturn()
            Responder responder = new Responder(channel, callID, 
                    writable.valueOf(3 * parameter.get()));
            responderExecutor.execute(responder);
        }

        private byte[] data;
        private SocketChannel channel;
    }

    private class Responder implements Runnable {

        public Responder(SocketChannel channel, Writable callID, Writable value) {
            this.channel = channel;
            this.callID = callID;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                // Construct reply main body
                DataOutputBuffer dataOut = new DataOutputBuffer();
                callID.write(dataOut);
                value.write(dataOut);
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
                if (Log.debug) Log.debug("Reply to .callID .length", callID, dataLength);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private SocketChannel channel;
        private Writable callID;
        private Writable value;
    }
}
