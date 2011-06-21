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
    public static final int FINAL = 3;

    public OutputChannelBuffer(SocketChannel channel) {
        this.channel = channel;
        state = FRAME_INIT;
        full = false;
    }
    
    public OutputChannelBuffer(SocketChannel channel, ByteBuffer data, boolean isFinal) {
        this(channel);
        putData(data, isFinal);
    }

    public void putData(ByteBuffer data, boolean isFinal) {
        try {
            queue.put(data);
            if (isFinal) full = true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    
    /**
     * @return Whether further waiting is necessary
     * */
    public boolean write() {
        while (true) {
            switch (state) {
            case FRAME_INIT:
                data = queue.poll();
                if (data != null) {
                    state = PUT_DATA;
                } else if (full) {
                    state = FINAL;
                } else return true;
                break;
            case PUT_DATA:
                try {
                    channel.write(data);
                } catch (IOException e) {
                    e.printStackTrace();
                    state = FINAL;
                    break;
                }
                if (!data.hasRemaining()) {
                    data = null;
                    state = FRAME_INIT;
                    
                    if (Log.DEBUG) Log.action(
                            "Finished some writing for Port # with # left",
                            channel.socket().getPort(), queue.size());
                } else return true;
                break;
            case FINAL:
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return false;
            default:
                throw new IllegalArgumentException(
                        "Invalid internal state in InputChannelBuffer.");
            }
        }
    }
    
    private int state;
    private SocketChannel channel;
    private LinkedBlockingQueue<ByteBuffer> queue = 
        new LinkedBlockingQueue<ByteBuffer>();
    private ByteBuffer data = null;
    private boolean full;
}
