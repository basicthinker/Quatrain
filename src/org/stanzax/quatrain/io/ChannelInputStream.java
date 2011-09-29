/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author Jinglei Ren
 * An InputStream implementation that allows a non-blocking socket channel
 */
public class ChannelInputStream extends InputStream {

    public ChannelInputStream(SocketChannel channel) {
        this.channel = channel;
        this.buf = ByteBuffer.allocate(1);
    }
    
    /* (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        buf.clear();
        while ((n = channel.read(buf)) == 0);
        return n == 1 ? buf.get(0) & 0xFF : -1;
    }
    
    private int n;
    private ByteBuffer buf;
    private SocketChannel channel;
}
