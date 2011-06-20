/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author basicthinker
 *
 */
public class OutputChannelBuffer {
    
    public static final int FRAME_INIT = 1;
    public static final int PUT_DATA = 2;

    public OutputChannelBuffer(SocketChannel channel) {
        this.channel = channel;
        state = FRAME_INIT;
    }
    
    public OutputChannelBuffer(SocketChannel channel, ByteBuffer data) {
        this(channel);
        this.putData(data);
    }

    public void putData(ByteBuffer data) {
        try {
            queue.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public void write() {
        try {
            while (true) {
                switch (state) {
                case FRAME_INIT:
                    data = queue.poll();
                    state = PUT_DATA;
                    break;
                case PUT_DATA:
                    channel.write(data);
                    if (!data.hasRemaining()) {
                        data = null;
                        state = FRAME_INIT;
                        break;
                    } else return;
                default:
                    throw new IllegalArgumentException(
                            "Invalid internal state in InputChannelBuffer.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private int state;
    private SocketChannel channel;
    private LinkedBlockingQueue<ByteBuffer> queue = 
        new LinkedBlockingQueue<ByteBuffer>();
    private ByteBuffer data = null;
}
