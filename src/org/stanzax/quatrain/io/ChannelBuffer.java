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
    
    /** Get the number of bytes stored in data buffer */
    public int getLength() {
        if (dataBuf == null) return 0;
        else return dataBuf.capacity();
    }
    
    /** Return data of complete frame and clear this channel buffer */
    public byte[] getData() {
        try {
            if (dataBuf == null || dataBuf.hasRemaining()) return null;
            else return dataBuf.array();
        } finally {
            clear();
        }
    }
    
    public SocketChannel getChannel() {
        return channel;
    }
    
    public boolean tryReadLength() {
        try {
            channel.read(lengthBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return !lengthBuf.hasRemaining();
    }
    
    /** Try reading from channel and return if new frame is ready */
    public boolean tryReadData() {
        if (dataBuf == null) {
            if (!lengthBuf.hasRemaining()) {
                lengthBuf.flip();
                dataBuf = ByteBuffer.allocate(lengthBuf.getInt());
            } else return false;
        }
        try {
            channel.read(dataBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return !dataBuf.hasRemaining();
    }
    
    public void clear() {
        lengthBuf.clear();
        dataBuf = null;
    }
    
    private SocketChannel channel;
    private ByteBuffer lengthBuf = ByteBuffer.allocate(4);
    private ByteBuffer dataBuf;
}
