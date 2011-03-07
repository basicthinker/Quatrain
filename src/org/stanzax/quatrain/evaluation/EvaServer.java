/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import org.stanzax.quatrain.io.WritableWrapper;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 *
 */
public class EvaServer extends MrServer {

    public static int step = 5;
    
    /**
     * @param address
     * @param port
     * @param handlerCount
     * @param responderCount
     * @throws IOException
     */
    public EvaServer(String address, int port, int handlerCount,
            int responderCount) throws IOException {
        super(address, port, handlerCount, responderCount);
    }

    /**
     * @param address
     * @param port
     * @param wrapper
     * @param handlerExecutor
     * @param responderExecutor
     * @throws IOException
     */
    public EvaServer(String address, int port, WritableWrapper wrapper,
            ThreadPoolExecutor handlerExecutor,
            ThreadPoolExecutor responderExecutor) throws IOException {
        super(address, port, wrapper, handlerExecutor, responderExecutor);
    }

    public void SequentialExecute(int workTime, int numParts) {
        int eachTime = (int) (workTime / numParts);
        long begin, end;
        for (int i = 0; i < numParts; ++i) {
            end = begin = System.currentTimeMillis();
            while (end - begin < eachTime) {
                try {
                    Thread.sleep(eachTime < step ? eachTime : step);
                    end = System.currentTimeMillis();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            preturn("3.1415926");
        }
    }
    
    public void ConcurrentExecute(int workTime, int numThreads) {
        for (int i = 0; i < numThreads; ++i) {
            final int eachTime = (int) (workTime / numThreads);
            new Thread(new Runnable() {
                public void run() {
                    long begin, end;
                    end = begin = System.currentTimeMillis();
                    while (end - begin < eachTime) {
                        try {
                            Thread.sleep(eachTime < step ? eachTime : step);
                            end = System.currentTimeMillis();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    preturn("3.1415926");
                }
            }).start();
        }
    }
    
    /**
     * @param args
     *          args[0] Remote server address
     *          args[1] Total work time to simulate
     *          args[2] Number of handlers
     *          args[3] Number of responders
     */
    public static void main(String[] args) {
        try {
            EvaServer server = new EvaServer(args[0], 3122, 
                    Integer.valueOf(args[1]), Integer.valueOf(args[2]));
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
