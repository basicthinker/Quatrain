/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.stanzax.quatrain.hadoop.HadoopWrapper;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 *
 */
public class HadoopServer extends EvaServer {

    /**
     * @param address
     * @param port
     * @param handlerCount
     * @throws IOException
     */
    public HadoopServer(String address, int port, 
            int handlerCount) throws IOException {
        super(address, port, new HadoopWrapper(), handlerCount);
    }

    /**
     * @param address
     * @param port
     * @param wrapper
     * @param handlerExecutor
     * @throws IOException
     */
    public HadoopServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor) throws IOException {
        super(address, port, wrapper, handlerExecutor);
    }
    
    /**
     * @param args
     *          args[0] Server address
     *          args[1] Port number
     *          args[2] Number of handlers
     *          args[3] Debug log option
     */
    public static void main(String[] args) {
        Log.setDebug(Integer.valueOf(args[3]));
        try {
            HadoopServer server = new HadoopServer(args[0], Integer.valueOf(args[1]), 
                    Integer.valueOf(args[2]));
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
