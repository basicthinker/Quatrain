/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ResultSet;
import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 * 
 */
public class SampleClient {

    private static void callback(Object returnValue) {
        System.out.println("Callback invoked : " 
                + returnValue.getClass().getName() + " : " + returnValue.toString());
    }

    public static void main(String[] args) {
        try {
            Log.setDebug(true);
            MrClient client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HadoopWrapper(), 
                    Long.valueOf(args[2]));
            // invoke non-blocking multi-return RPC
            ResultSet records = client.invoke("functionName", Integer.TYPE);
            // incrementally retrieve partial returns
            while (records.hasMore()) {
                // do work on partial returns
                callback(records.nextElement());
                if (records.isPartial())
                    System.out.println("Still working...");
            }
            // judge whether all returns arrive
            if (records.isPartial())
                Log.info("Other results are omitted.");
            else
                Log.info("Fully completed.");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
