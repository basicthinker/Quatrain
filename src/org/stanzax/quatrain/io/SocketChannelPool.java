/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author basicthinker
 * 
 */
public class SocketChannelPool {

    public SocketChannelPool() {
        pool = new ConcurrentHashMap<SocketAddress, ManagedSocketChannel>();
    }

    public SocketChannel getSocketChannel(SocketAddress remoteAddress)
            throws IOException {
        ManagedSocketChannel managedChannel = pool.get(remoteAddress);
        if (managedChannel != null)
            return managedChannel.getSocketChannel();
        else {
            SocketChannel channel = SocketChannel.open(remoteAddress);
            pool.put(remoteAddress, new ManagedSocketChannel(channel));
            return channel;
        }
    }

    public void putSocketChannel(SocketChannel channel) {
        SocketAddress address = channel.socket().getRemoteSocketAddress();
        if (!channel.isOpen() || !channel.isConnected())
            pool.remove(address);
        else if (pool.contains(address)) {
            pool.get(channel.socket().getRemoteSocketAddress()).setInactive();
        } else
            return;
    }

    /** Thread-safe hash table to restore established socket channels */
    private ConcurrentHashMap<SocketAddress, ManagedSocketChannel> pool;

    /** Socket channel with time stamp and other management facilities */
    private class ManagedSocketChannel {

        public ManagedSocketChannel(SocketChannel channel) {
            this.channel = channel;
            this.lastActiveTime = System.currentTimeMillis();
        }

        public SocketChannel getSocketChannel() {
            return channel;
        }

        public void setInactive() {
            lastActiveTime = System.currentTimeMillis();
        }

        public long getLastActiveTime() {
            return lastActiveTime;
        }

        private SocketChannel channel;
        /** The time this socket channel was given back to pool */
        private volatile long lastActiveTime;
    }
}
