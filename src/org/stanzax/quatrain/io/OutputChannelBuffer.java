/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author basicthinker
 *
 */
public class OutputChannelBuffer {
    
    public static final int PUT_LEN = 1;
    public static final int PUT_DATA = 2;
    public static final int FRAME_END = 3;

    public OutputChannelBuffer(SocketChannel channel, ReentrantLock lock, ByteBuffer length, ByteBuffer data) {
        this.channel = channel;
        this.lock = lock;
        this.length = length;
        this.data = data;
        state = FRAME_END;
    }
    
    public void write() {
        if (lock.isLocked()) return;
        try {
            while (true) {
                switch (state) {
                case PUT_LEN:
                    lock.lock();
                    channel.write(length);
                    if (!length.hasRemaining()) {
                        length = null;
                        state = PUT_DATA;
                        break;
                    } else return;
                case PUT_DATA:
                    channel.write(data);
                    if (!data.hasRemaining()) {
                        data = null;
                        state = FRAME_END;
                        break;
                    } else return;
                case FRAME_END:
                    lock.unlock();
                    return;
                default:
                    lock.unlock();
                    throw new IllegalArgumentException(
                            "Invalid internal state in InputChannelBuffer.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            lock.unlock();
        }
    }
    
    private int state;
    private SocketChannel channel;
    private ReentrantLock lock;
    private ByteBuffer length;
    private ByteBuffer data;
}
