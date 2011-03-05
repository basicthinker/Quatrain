/**
 * 
 */
package org.stanzax.quatrain.sample;

import java.io.IOException;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 * 
 */
public class SampleServer extends MrServer {

    public SampleServer(String address, int port, int handlerCount,
            int responderCount) throws IOException {
        super(address, port, handlerCount, responderCount);
    }


    /** Remotely called procedure */
    public void SampleProcedure(int count) {
        for (int i = 0; i < count; ++i) {
            new Thread(new Runnable() {
                public void run() {
                    preturn("3.1415926");
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
            SampleServer server = new SampleServer("localhost", 3122, 3, 5);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
