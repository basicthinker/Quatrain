/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.hadoop.HadoopWrapper;
import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 *
 */
public class HadoopEva {

    /**
     * @param args
     *          args[0] Remote server address
     *          args[1] Remote port number
     *          args[2] Total work time to simulate
     *          args[3] Max number of parts server divides whole task into
     *          args[4] Number of loops to execute evaluation
     *          args[5] Debug log option
     */
    public static void main(String[] args) {
        Log.setDebug(Integer.valueOf(args[5]));
        MrClient client;
        try {
            client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HadoopWrapper(), 6000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        int taskTime = Integer.valueOf(args[2]);
        int returnCount = Integer.valueOf(args[3]);
        int evaCount = Integer.valueOf(args[4]);
        
        System.out.println("Evaluation Parameters:");
        System.out.println("Total work time =\t" + taskTime);
        System.out.println("Max return count =\t" + returnCount);
        System.out.println("Max execution count =\t" + evaCount);
        
        try {
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter("log/HadoopEva@" + EvaClient.currentTime() + ".log"));
            EvaClient evaClient = new EvaClient(client, writer);evaClient.setTaskTime(taskTime);
            evaClient.setReturnCount(returnCount);
            evaClient.setEvaCount(evaCount);
            
            evaClient.testSRSE();
            evaClient.testSRPE();
            
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
