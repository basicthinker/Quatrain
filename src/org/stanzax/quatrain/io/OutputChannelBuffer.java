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
    }
    
    public OutputChannelBuffer(SocketChannel channel, ByteBuffer data, boolean isFinal) {
        this(channel);
        putData(data, isFinal);
    }

    public void putData(ByteBuffer data, boolean isFinal) {
        try {
            queue.put(new WriteData(data, isFinal));
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
                wdata = queue.poll();
                if (wdata != null) {
                    state = PUT_DATA;
                } else return true;
                break;
            case PUT_DATA:
                try {
                    channel.write(wdata.data);
                } catch (IOException e) {
                    e.printStackTrace();
                    state = FINAL;
                    break;
                }
                if (!wdata.data.hasRemaining()) {
                    if (!wdata.isFinal)
                        state = FRAME_INIT;
                    else state = FINAL;
                    wdata.data = null;
                } else return true;
                break;
            case FINAL:
                try {
                    channel.socket().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
    private LinkedBlockingQueue<WriteData> queue = 
        new LinkedBlockingQueue<WriteData>();
    private WriteData wdata = null;
    
    class WriteData {
        public WriteData(ByteBuffer data, boolean isFinal) {
            this.data = data;
            this.isFinal = isFinal;
        }
        public ByteBuffer data;
        public boolean isFinal;
    }
}
