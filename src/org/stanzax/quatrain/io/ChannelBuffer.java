/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author basicthinker
 *
 */
public class ChannelBuffer {

    public ChannelBuffer(SocketChannel channel) {
        this.channel = channel;
    }
    
    public boolean hasLength() {
        return !lengthBuf.hasRemaining();
    }
    
    /** Get the number of bytes */
    public int getLength() {
        return dataBuf.capacity();
    }
    
    public byte[] getData() throws IOException {
        if (dataBuf == null) return null;
        else if (dataBuf.hasRemaining())
            throw new IOException("ChannelBuffer: getData() before data are ready.");
        else return dataBuf.array();
    }
    
    public SocketChannel getChannel() {
        return channel;
    }
    
    public boolean tryReadLength() throws IOException {
        channel.read(lengthBuf);
        return !lengthBuf.hasRemaining();
    }
    
    public boolean tryReadData() throws IOException {
        if (dataBuf == null) {
            if (!lengthBuf.hasRemaining()) {
                lengthBuf.flip();
                dataBuf = ByteBuffer.allocate(lengthBuf.getInt());
            } else throw new IOException(
                    "ChannelBuffer: tryReadData() before length is ready.");
        }
        if (channel.read(dataBuf) > 0) {
            return !dataBuf.hasRemaining();
        } else return false;
    }
    
    private SocketChannel channel;
    private ByteBuffer lengthBuf = ByteBuffer.allocate(4);
    private ByteBuffer dataBuf;
}
