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
        System.out.println(returnValue);
    }

    public static void main(String[] args) {
        try {
            Log.setDebug(true);
            MrClient client = new MrClient(InetAddress.getByName("localhost"),
                    3122, new HadoopWrapper(), 16000);
            // invoke non-blocking multi-return RPC
            ResultSet records = client.invoke("functionName", String.class);
            // incrementally retrieve partial returns
            while (records.hasMore()) {
                // do work on partial returns
                callback(records.nextElement());
                if (records.isPartial())
                    Log.info("Still working...");
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
