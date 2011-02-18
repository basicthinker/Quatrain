/**
 * 
 */
package org.stanzax.quatrain.client;

import java.net.InetAddress;

import javax.net.SocketFactory;

/**
 * @author basicthinker
 * 
 */
public class MrClient {

    public MrClient(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /**
     * Sets server to bind for the client
     * 
     * @param host
     *            Internet address of target server
     * @param port
     *            target port number on the server
     */
    public void useRemote(InetAddress host, int port) {
        // TODO Method stub
    }

    public <ElementType> ResultSet<ElementType> invoke(String functionName,
            int numLinks) {
        // TODO Method stub
        return new ResultSet<ElementType>();
    }

    public <ElementType> ResultSet<ElementType> invoke(String functionName) {
        return invoke(functionName, 1);
    }

    public <ElementType> ResultSet<ElementType> invoke(String functionName,
            Object[] arguments, int numLinks) {
        // TODO Method stub
        return new ResultSet<ElementType>();
    }

    public <ElementType> ResultSet<ElementType> invoke(String functionName,
            Object[] arguments) {
        return invoke(functionName, arguments, 1);
    }

    private SocketFactory socketFactory;
}
