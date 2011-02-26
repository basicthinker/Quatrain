/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.stanzax.quatrain.io.DataOutputBuffer;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.SocketChannelPool;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 * 
 */
public class MrClient {

    public MrClient(SocketAddress address, WritableWrapper wrapper) {
        this.address = address;
        this.writable = wrapper;
    }

    public MrClient(InetAddress host, int port, WritableWrapper wrapper) {
        this.address = new InetSocketAddress(host, port);
        this.writable = wrapper;
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

    public <ElementType> ResultSet<ElementType> invoke(String functionName) {
        return invoke(functionName, new Object[0]);
    }

    public <ElementType> ResultSet<ElementType> invoke(String functionName,
            Object[] arguments) {
        try {
            SocketChannel channel = channelPool.getSocketChannel(address);
            DataOutputBuffer dataOut = new DataOutputBuffer();
            writable.valueOf(48579).write(dataOut);
            dataOut.flush();
            
            int dataLength = dataOut.getDataLength();
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4).putInt(dataLength);
            lengthBuffer.flip();
            
            synchronized(channel) {
                channel.write(lengthBuffer);
                channel.write(ByteBuffer.wrap(dataOut.getData(),
                        0, dataLength));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ResultSet<ElementType>();
    }

    /** Hold a socket address of the target server */
    private SocketAddress address;
    private WritableWrapper writable;
    /** Hold a static socket channel pool */
    private static SocketChannelPool channelPool = new SocketChannelPool();
    
    
}
