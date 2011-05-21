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

    public static final int FILL_LEN = 1;
    public static final int FILL_DATA = 2;
    public static final int FINAL = 3;

    public ChannelBuffer(SocketChannel channel) {
        this.channel = channel;
        state = FILL_LEN; // prepared for receiving input
    }
    
    public byte[] read() {
        while (true) {
            try {
                switch (state) {
                case FILL_LEN:
                    channel.read(lengthBuffer);
                    if (lengthBuffer.hasRemaining()) {
                        return null;
                    } else {
                        lengthBuffer.flip();
                        dataBuffer = ByteBuffer.allocate(lengthBuffer.getInt());
                        state = FILL_DATA;
                        break;
                    }
                case FILL_DATA:
                    channel.read(dataBuffer);
                    if (dataBuffer.hasRemaining()) {
                        return null;
                    } else {
                        state = FINAL;
                        return dataBuffer.array();
                    }
                case FINAL:
                    lengthBuffer.clear();
                    dataBuffer = null;
                    state = FILL_LEN;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Invalid internal state in ChannelBuffer.");
                }                
            } catch (IOException e) {
                e.printStackTrace();
            }
        } // main while
    }
    
    public SocketChannel getChannel() {
        return channel;
    }
    
    private SocketChannel channel;
    private ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
    private ByteBuffer dataBuffer;
    private int state;
}
