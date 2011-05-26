/**
 * 
 */
package org.stanzax.quatrain.sample;

import java.io.IOException;

import org.stanzax.quatrain.hprose.HproseWrapper;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.WritableWrapper;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 * 
 */
public class SampleServer extends MrServer {

    public SampleServer(String address, int port, WritableWrapper wrapper,
            int handlerCount) throws IOException {
        super(address, port, wrapper, handlerCount);
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
            SampleServer server = new SampleServer("localhost", 3122, new HproseWrapper(), 10);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
