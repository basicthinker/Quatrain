/**
 * 
 */
package org.stanzax.quatrain.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author basicthinker
 *
 */
public class ReplyData {

    public ReplyData(SocketChannel channel, ByteBuffer data, boolean isFinal) {
        this.channel = channel;
        this.data = data;
        this.isFinal = isFinal;
    }
    
    public SocketChannel channel;
    public ByteBuffer data;
    public boolean isFinal;
}
