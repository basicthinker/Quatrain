/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Map;
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

    public int size() {
        return pool.size();
    }
    
    public void clear() {
        for (Map.Entry<SocketAddress, ManagedSocketChannel> entry : pool.entrySet()) {
            entry.getValue().close();
        }
        pool.clear();
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

        /** Try to close both this channel and its underlying socket */
        public void close() {
            try {
                channel.socket().close();
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        private SocketChannel channel;
        /** The time this socket channel was given back to pool */
        private volatile long lastActiveTime;
    }
}
