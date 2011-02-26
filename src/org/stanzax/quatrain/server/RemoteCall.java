/**
 * 
 */
package org.stanzax.quatrain.server;

import java.nio.channels.SocketChannel;

/**
 * @author basicthinker
 * 
 */
public class RemoteCall {

    public RemoteCall(long callID, SocketChannel channel) {
        this.callID = callID;
        this.channel = channel;
    }

    public long getID() {
        return callID;
    }

    public SocketChannel getSocketChannel() {
        return channel;
    }

    public void setCallName(String name) {
        this.name = name;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    private long callID;
    private SocketChannel channel;
    private String name;
    private Object[] parameters;
}
