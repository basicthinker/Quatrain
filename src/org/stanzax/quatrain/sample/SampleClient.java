/**
 * 
 */
package org.stanzax.quatrain.sample;

import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ReplySet;
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
     * @param args
     */
    public static void main(String[] args) {
        try {
            // Set log options, combination of NONE, ACTION and STATE
            Log.setDebug(Log.ACTION + Log.STATE);
            
            MrClient client = new MrClient(InetAddress.getByName("localhost"),
            		3122, 10000, null);

            // Invoke non-blocking multi-return RPC.
            // The sample procedure returns pi strings with number equal to parameter
            expected = 5;
            ReplySet records = client.invoke(Text.class,
            		"SampleProcedure", new IntWritable(expected));

            // incrementally retrieve partial returns
            while (records.hasMore()) {
                // do work on each partial return
                doWorkOn(records.nextElement().toString());
            }
            // judge whether all returns arrive
            if (records.isPartial())
                Log.info("Some results are omitted.");
            else // check the returns are correct or not
                Log.info(expected == 0 ? "Check" : "*!WRONG!*", expected);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
