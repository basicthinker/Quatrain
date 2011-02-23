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
    		ThreadPoolExecutor responderExecutor) 
    throws IOException {
        this.bindAddress = new InetSocketAddress(address, port); // set up bind address
        this.channel = new InheritableThreadLocal<SocketChannel>();
        this.callID = new InheritableThreadLocal<Long>();
        
        this.listener = new Thread(new Listener()); // create listener thread
        this.listener.setDaemon(true);
        
        this.handlerExecutor = handlerExecutor;
        this.responderExecutor = responderExecutor;
    }

    public void start() {
        try {
        	isRunning = true;
	        listener.start();
	        Log.info("Quatrain Service Starts.");
			listener.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
    }

    public void stop() {

    }

    protected void preturn(Writable value) {
    	
    }
    protected void preturn(int intValue) {
    	
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
    protected WritableWrapper wrapper;
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
            // Allocate input buffer
            inBuffer = ByteBuffer.allocate(1024);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    if (selector.select() > 0) { // if there exist new events
                        Set<SelectionKey> activeKeys = selector.selectedKeys();
                        for (SelectionKey key : activeKeys) {
                            if (key.isAcceptable()) {
                                // retrieve the associated acceptance channel
                                ServerSocketChannel acceptChannel = (ServerSocketChannel) key.channel();
                                // create connection and register reading channel
                                SocketChannel readChannel = acceptChannel.accept();
                                if (readChannel != null) {
                                    readChannel.configureBlocking(false);
                                    readChannel.register(selector, SelectionKey.OP_READ);
                                }
                            } else if (key.isReadable()) {
                                doRead((SocketChannel)key.channel());
                            }
                        }
                        activeKeys.clear();
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        private void doRead(SocketChannel channel) throws IOException {
            // TODO Auto-generated method stub
            while (channel.read(inBuffer) > 0) {
	            inBuffer.flip();
	            DataInputStream dataStream = new DataInputStream(new ByteArrayInputStream(inBuffer.array()));
	            inBuffer.clear();
	            String data = dataStream.readUTF();
	            System.out.println(data);
            }
        }

        /** Hold one selector to tell socket events */
        private Selector selector;
        /** Hold a byte buffer for input, as class field to avoid multiple allocation */
        ByteBuffer inBuffer;
    }

    private class Handler implements Runnable {

    	public Handler(RemoteCall call) {
    		this.call = call;
    	}
    	
		@Override
		public void run() {
			// TODO Auto-generated method stub
			Log.info(getClass().getName());
			String value = this.getClass().getEnclosingClass().getName();
			Responder responder = new Responder(call.getID(), call.getSocketChannel(), wrapper.valueOf(value));
			responderExecutor.execute(responder);
		}
    	
		private RemoteCall call;
    }
    
    private class Responder implements Runnable {

    	public Responder(long callID, SocketChannel channel, Writable value) {
    		this.callID = callID;
    		this.channel = channel;
    		this.value = value;
    	}
    	
        @Override
        public void run() {
            // TODO Auto-generated method stub
            
        }
        
    	private long callID;
    	private SocketChannel channel;
    	private Writable value;
    }
}
