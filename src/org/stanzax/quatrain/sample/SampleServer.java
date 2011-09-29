/**
 * 
 */
package org.stanzax.quatrain.sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 * 
 */
public class SampleServer extends MrServer {

    public SampleServer(String address, int port,
            int handlerCount, Configuration conf) throws IOException {
        super(address, port, handlerCount, conf);
    }


    /** Remotely called procedure */
    public void SampleProcedure(IntWritable count) {
        for (int i = 0; i < count.get(); ++i) {
            new Thread(new Runnable() {
                public void run() {
                    preturn(new Text("3.1415926"));
                }
            }).start();
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // Set log options, combination of NONE, ACTION and STATE
            Log.setDebug(Log.ACTION + Log.STATE);
            SampleServer server = new SampleServer("localhost", 3122, 10, null);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
