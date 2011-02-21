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
import java.util.UUID;

import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 * 
 */
public class MrServer {

    public MrServer(String address, int port, int handlerCount) throws IOException {
        bindAddress = new InetSocketAddress(address, port); // set up bind address
        listener = new Thread(new Listener()); // create listener thread
        listener.setDaemon(true);
        responder = new Thread(new Responder()); // create responder thread
        responder.setDaemon(true);
        // TODO Multiple handlers

    }

    public void start() {
        try {
        	isRunning = true;
	        responder.start();
	        listener.start();
	        Log.info("Quatrain Service Starts.");
			listener.join();
			responder.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
    }

    public void stop() {

    }

    /**
     * Returns partical results
     * 
     * @param value
     *            the partical return value
     */
    protected void preturn(Object value) {
        // TODO Method stub
        UUID requestID = REQUEST_ID.get();
    }

    InetSocketAddress bindAddress;

    /** Each server contains one thread listening to target socket address */
    Thread listener; 
    /** Each server contains one thread sending back replies */
    Thread responder;
    
    /** Denotes whether this server is still running */
    private volatile boolean isRunning;

    /** Set by Handler and retrieved by preturn() to locate the respond target */
    protected static final InheritableThreadLocal<UUID> REQUEST_ID = new InheritableThreadLocal<UUID>();

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
    
    private class Responder implements Runnable {
        
        public Responder() {
            
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            
        }
    }
}
