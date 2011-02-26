/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ResultSet;

/**
 * @author basicthinker
 * 
 */
public class SampleClient {

    private static void callback(String returnValue) {
        System.out.println(returnValue);
    }

    public static void main(String[] args) {
        try {
            MrClient client = new MrClient(InetAddress.getByName("localhost"),
                    3122, new HadoopWrapper());
            // invoke non-blocking multi-return RPC
            ResultSet<String> records = client.invoke("functionName");
            // incrementally retrieve partial returns
            while (records.hasMoreElements()) {
                // do work on partial returns
                callback(records.nextElement());
                if (records.isPartial())
                    System.out.println("Still working...");
            }
            // judge whether all returns arrive
            if (records.isPartial())
                System.out.println("Other results are omitted.");
            else
                System.out.println("Fully completed.");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
