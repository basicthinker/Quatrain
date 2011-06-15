/**
 * 
 */
package org.stanzax.quatrain.sample;

import java.net.InetAddress;
import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ReplySet;
import org.stanzax.quatrain.hadoop.HadoopWrapper;
import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 * 
 */
public class SampleClient {

    static int expected;
    
    private static void doWorkOn(Object returnValue) {
        if (returnValue.equals("3.1415926")) --expected; // one return arrives
        else Log.info("WRONG RETURN", returnValue.toString());
    }

    /**
     * @param args[0] server host name
     * @param args[1] server port number
     * @param args[2] timeout for one RPC to return
     */
    public static void main(String[] args) {
        try {
            // Set log options, combination of NONE, ACTION and STATE
            Log.setDebug(Log.ACTION + Log.STATE);
            
            MrClient client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HadoopWrapper(), 
                    Long.valueOf(args[2]));

            int parameter = (int)(Math.random() * 20); // prepare a random parameter
            // Invoke non-blocking multi-return RPC.
            // The sample procedure returns pi strings with number equal to parameter
            expected = parameter; // for checking whether all returns arrive
            ReplySet records = client.invoke(Double.class, "SampleProcedure", parameter);

            // incrementally retrieve partial returns
            while (records.hasMore()) {
                // do work on each partial return
                doWorkOn(records.nextElement());
            }
            // judge whether all returns arrive
            if (records.isPartial())
                Log.info("Some results are omitted.");
            else // check the returns are correct or not
                Log.info(expected == 0 ? "Check" : "*!WRONG!*", parameter, expected);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
