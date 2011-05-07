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

    public EvaServer(String address, int port, WritableWrapper wrapper,
            int handlerCount, int responderCount) throws IOException {
        super(address, port, wrapper, handlerCount, responderCount);
    }

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
                end = System.currentTimeMillis();
            }
            preturn("3.1415926");
        }
    }
    
    public void ParallelExecute(int workTime, int numThreads) {
        for (int i = 0; i < numThreads; ++i) {
            final int eachTime = (int) (workTime / numThreads);
            new Thread(new Runnable() {
                public void run() {
                    long begin, end;
                    end = begin = System.currentTimeMillis();
                    while (end - begin < eachTime) {
                        end = System.currentTimeMillis();
                    }
                    preturn("3.1415926");
                }
            }).start();
        }
    }
}
