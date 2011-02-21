/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.SocketChannelPool;

/**
 * @author basicthinker
 * 
 */
public class MrClient {

    public MrClient(SocketAddress address) {
        this.address = address;
    }

    public MrClient(InetAddress host, int port) {
    	this.address = new InetSocketAddress(host, port);
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
    	// TODO Serialization
    	try {
			SocketChannel channel = channelPool.getSocketChannel(address);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DataOutputStream serializer = new DataOutputStream(out);
			serializer.writeUTF(functionName);
			channel.write(ByteBuffer.wrap(out.getByteArray(), 0, out.size()));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return new ResultSet<ElementType>();
    }

    /** Hold a socket address of the target server */
    private SocketAddress address;
    /** Hold a static socket channel pool */
    private static SocketChannelPool channelPool = new SocketChannelPool();
    
    /** ByteArrayOutputStream that give direct access to its backing array */
    private class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
    	public byte[] getByteArray() {
    		return buf;
    	}
    }
}
