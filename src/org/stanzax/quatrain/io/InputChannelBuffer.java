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
public class InputChannelBuffer {

    public static final int FILL_LEN = 1;
    public static final int FILL_DATA = 2;
    public static final int FINAL = 3;

    public InputChannelBuffer(SocketChannel channel) {
        this.channel = channel;
        state = FILL_LEN; // prepared for receiving input
    }
    
    public byte[] read() throws IOException {
        while (true) {
            switch (state) {
            case FILL_LEN:
                channel.read(lengthBuffer);
                if (!lengthBuffer.hasRemaining()) {
                    lengthBuffer.flip();
                    dataBuffer = ByteBuffer.allocate(lengthBuffer.getInt());
                    state = FILL_DATA;
                    break;
                } else return null;
            case FILL_DATA:
                channel.read(dataBuffer);
                if (!dataBuffer.hasRemaining()) {
                    state = FINAL;
                    break;
                } else return null;
            case FINAL:
                lengthBuffer.clear();
                byte[] data = dataBuffer.array();
                dataBuffer = null;
                state = FILL_LEN;
                return data;
            default:
                throw new IllegalArgumentException(
                        "Invalid internal state in InputChannelBuffer.");
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
