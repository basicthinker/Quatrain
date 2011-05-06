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

    public static int step = 5;
    
    /**
     * @param address
     * @param port
     * @param handlerCount
     * @param responderCount
     * @throws IOException
     */
    public HadoopServer(String address, int port, int handlerCount,
            int responderCount) throws IOException {
        super(address, port, new HadoopWrapper(), handlerCount, responderCount);
    }

    /**
     * @param address
     * @param port
     * @param wrapper
     * @param handlerExecutor
     * @param responderExecutor
     * @throws IOException
     */
    public HadoopServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor,
            ThreadPoolExecutor responderExecutor) throws IOException {
        super(address, port, wrapper, handlerExecutor, responderExecutor);
    }
    
    /**
     * @param args
     *          args[0] Server address
     *          args[1] Port number
     *          args[2] Number of handlers
     *          args[3] Number of responders
     *          args[4] Debug log option
     */
    public static void main(String[] args) {
        Log.setDebug(Integer.valueOf(args[4]));
        try {
            HadoopServer server = new HadoopServer(args[0], Integer.valueOf(args[1]), 
                    Integer.valueOf(args[2]), Integer.valueOf(args[3]));
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
