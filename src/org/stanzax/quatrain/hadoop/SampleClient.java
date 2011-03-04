/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.net.InetAddress;
import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ResultSet;
import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 * 
 */
public class SampleClient {

    static int expected;
    
    private static void callback(Object returnValue) {
        if (returnValue.equals("3.1415926")) --expected;
        else Log.info("Return Contenct Wrong", returnValue.toString());
    }

    /**
     * @param args[0] server host name
     * @param args[1] server port number
     * @param args[2] timeout for one RPC to return
     */
    public static void main(String[] args) {
        try {
            // Log.setDebug(true);
            MrClient client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HadoopWrapper(), 
                    Long.valueOf(args[2]));

            int parameter = (int)(Math.random() * 20);
            expected = parameter;
            // invoke non-blocking multi-return RPC
            ResultSet records = client.invoke(String.class, "ProcedureName", parameter);

            // incrementally retrieve partial returns
            while (records.hasMore()) {
                // do work on partial returns
                callback(records.nextElement());
            }
            // judge whether all returns arrive
            if (records.isPartial())
                Log.info("Results are omitted.");
            else
                Log.info(expected == 0 ? "Check" : "*!WRONG!*", parameter, expected);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
