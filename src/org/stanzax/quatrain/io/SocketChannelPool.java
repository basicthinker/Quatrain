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

    public SocketChannelPool(boolean forceNew) {
        this.pool = new ConcurrentHashMap<SocketAddress, ManagedSocketChannel>();
        this.forceNew = forceNew;
    }

    public SocketChannel getSocketChannel(SocketAddress remoteAddress)
            throws IOException {
        if (!forceNew) {
            ManagedSocketChannel managedChannel = pool.get(remoteAddress);
            if (managedChannel == null) {
                SocketChannel channel = SocketChannel.open(remoteAddress);
                pool.put(remoteAddress, new ManagedSocketChannel(channel));
                return channel;           
            } else return managedChannel.getSocketChannel();
        } else return SocketChannel.open(remoteAddress);
    }

    public void putSocketChannel(SocketChannel channel) {
        if (forceNew || !channel.isOpen() || !channel.isConnected()) {
            try {
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            } 
            try {
                channel.close();
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            ManagedSocketChannel pooledChannel = pool.get(channel.socket().getRemoteSocketAddress());
            if (pooledChannel != null) pooledChannel.setInactive();
        } 
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

    private boolean forceNew;
    /** Thread-safe hash table to restore established socket channels */
    private ConcurrentHashMap<SocketAddress, ManagedSocketChannel> pool;

    /** 
     * Socket channel with time stamp and other management facilities 
     */
    private class ManagedSocketChannel {

        public ManagedSocketChannel(SocketChannel channel) {
            this.channel = channel;
            // this.lastActiveTime = System.currentTimeMillis();
        }

        public SocketChannel getSocketChannel() {
            return channel;
        }
        
        public void setInactive() {
            // lastActiveTime = System.currentTimeMillis();
        }

        /*
        public long getLastActiveTime() {
            return lastActiveTime;
        }
        */
        
        /** Try to close both this channel and its underlying socket */
        public void close() {
            try {
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                channel.close();
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        private SocketChannel channel;
        /** The time this socket channel was given back to pool */
        // private volatile long lastActiveTime;
    }
}
