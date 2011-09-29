/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.stanzax.quatrain.io.Array;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 *
 */
public class EvaServer extends MrServer {

    public EvaServer(String address, int port, int handlerCount, Configuration conf)
    		throws IOException {
        super(address, port, handlerCount, conf);
    }

    public EvaServer(String address, int port, 
    		ThreadPoolExecutor handlerExecutor, Configuration conf) throws IOException {
        super(address, port, handlerExecutor, conf);
    }

    public void SequentialExecute(IntWritable overall, IntWritable divider) {
    	int total = overall.get();
    	int div = divider.get();
        for (int i = 0; i < div; ++i) {
            int each = total / div + (i < total % div ? 1 : 0);
            DoubleWritable[] replies = new DoubleWritable[each];
            for (int j = 0; j < each; ++j) 
                replies[j] = new DoubleWritable(Math.PI);
            try {
                Thread.sleep(each);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            preturn(new Array(DoubleWritable.class, replies));        
        }
    }
    
    public void ParallelExecute(IntWritable overall, IntWritable divider) {
    	int total = overall.get();
    	int div = divider.get();
        for (int i = 0; i < div; ++i) {
            final int each = total / div + (i < total % div ? 1 : 0);
            final DoubleWritable[] replies = new DoubleWritable[each];
            for (int j = 0; j < each; ++j)
                replies[j] = new DoubleWritable(Math.PI);
            
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(each);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    preturn(new Array(DoubleWritable.class, replies));
                }
            }).start();
        }
    }
}
