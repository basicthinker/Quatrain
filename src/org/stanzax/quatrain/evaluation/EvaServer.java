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

    public void SequentialExecute(int total, int divider) {
        for (int i = 0; i < divider; ++i) {
            int each = total / divider + (i < total % divider ? 1 : 0);
            Double[] replies = new Double[each];
            for (int j = 0; j < each; ++j) 
                replies[j] = Math.PI;
            try {
                Thread.sleep(each);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            preturn(replies);        
        }
    }
    
    public void ParallelExecute(int total, int divider) {
        for (int i = 0; i < divider; ++i) {
            final int each = total / divider + (i < total % divider ? 1 : 0);
            final Double[] replies = new Double[each];
            for (int j = 0; j < each; ++j)
                replies[j] = Math.PI;
            
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(each);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    preturn(replies);
                }
            }).start();
        }
    }
}
