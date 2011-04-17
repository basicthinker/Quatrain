/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.stanzax.quatrain.io.ChannelBuffer;
import org.stanzax.quatrain.io.DataOutputBuffer;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.SocketChannelPool;
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
    
    public MrClient(SocketAddress address, WritableWrapper wrapper, long timeout) 
    throws IOException {
        this.address = address;
        this.writable = wrapper;
        this.timeout = timeout;
        listener.setDaemon(true);
        listener.start();   
    }

    /**
     * Set binded server for this client
     * 
     * @param address
     *            Socket address of target server
     */
    public void useRemote(SocketAddress address) {
        this.address = address;
    }
    
    public ResultSet invoke(Type returnType, String procedureName) {
        return invoke(timeout, returnType, procedureName, new Object[0]);
    }
    
    public ResultSet invoke(int timeout, Type returnType, String procedureName) {
        return invoke(timeout, returnType, procedureName, new Object[0]);
    }
    
    public ResultSet invoke(Type returnType, String procedureName,
            Object...parameters) {
        return invoke(timeout, returnType, procedureName, parameters);
    }
    
    public ResultSet invoke(long timeout, Type returnType, String procedureName,
            Object...parameters) {
        int callID = counter.addAndGet(1);
        // Early create and register result set for awaiting reply
        ResultSet results = new ResultSet(writable.newInstance(returnType), timeout);
        results.register(callID);
        try {
            SocketChannel channel = channelPool.getSocketChannel(address);
            DataOutputBuffer dataOut = new DataOutputBuffer();
            dataOut.writeInt(callID); //write call ID
            writable.valueOf(procedureName).write(dataOut); //write the procedure name
            for (Object parameter : parameters) { //write procedure parameters
                writable.valueOf(parameter).write(dataOut);
            }
            dataOut.flush();
            
            int dataLength = dataOut.getDataLength();
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4).putInt(dataLength);
            lengthBuffer.flip();
            // Send RPC request
            synchronized(channel) {
                channel.write(lengthBuffer);
                channel.write(ByteBuffer.wrap(dataOut.getData(),
                        0, dataLength));
            }
            if (Log.debug) Log.action("Send request with .callID .length", 
                    callID, dataLength);
            // Register for reply
            channel.configureBlocking(false);
            SelectionKey readKey = channel.register(selector, SelectionKey.OP_READ);
            readKey.attach(new ChannelBuffer(channel));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /** Hold a socket address of the target server */
    private SocketAddress address;
    private WritableWrapper writable;
    /** Max time in millisecond to wait for a return */
    private long timeout;
    /** Hold a selector waiting for reply */
    private Selector selector = Selector.open();
    /** Hold a Listener thread waiting for reply */
    private Thread listener = new Thread(new Listener());
    
    /** Static call ID counter */
    private static AtomicInteger counter = new AtomicInteger();
    /** Static socket channel pool */
    private static SocketChannelPool channelPool = new SocketChannelPool();
    
    private class Listener implements Runnable {

        @Override
        public void run() {
            Log.info("Client listener starts.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (selector.selectNow() > 0) { // non-blocking in order to go together with registration
                        Set<SelectionKey> selectedKeys = 
                            selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            if (Log.debug) Log.state(1, "Select keys ...");
                            if (key.isReadable()) {
                                ChannelBuffer channel = 
                                    (ChannelBuffer) key.attachment();
                                if (channel != null) doRead(channel);
                            }
                        }
                        selectedKeys.clear();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // Release occupied connection resources
            channelPool.clear();
            Log.info("Client listener shuts down (.connection pool size)",
                    channelPool.size());
        }
        
        /** Read complete remote call replies */
        private void doRead(ChannelBuffer channelBuffer) throws IOException {
            if (channelBuffer.hasLength() || channelBuffer.tryReadLength()) {
                if (channelBuffer.tryReadData()) {
                    if (Log.debug) Log.action("Receive reply with .length",
                            channelBuffer.getLength());
                    DataInputStream dataIn = new DataInputStream(
                            new ByteArrayInputStream(channelBuffer.getData()));
                    // Read in call ID
                    int callID = dataIn.readInt();
                    // Pass on data to corresponding result set
                    ResultSet results = ResultSet.get(callID);
                    if (results != null) {
                        results.putData(dataIn);
                        if (Log.debug) Log.action("Result set read in data.");
                    }
                } 
            } 
        }
    }
}
