/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.hprose.HproseWrapper;
import org.stanzax.quatrain.io.Log;

/**
 * @author basicthinker
 *
 */
public class HproseEva {

    /**
     * @param args
     *          args[0] Remote server address
     *          args[1] Remote port number
     *          args[2] Total work time to simulate
     *          args[3] Max number of parts server divides whole task into
     *          args[4] Number of loops to execute SR evaluation
     *          args[5] Number of Dispatchers
     *          args[6] Request per second for SE
     *          args[7] Request per second for PE
     *          args[8] Durition in second
     *          args[9] Debug log option
     */
    public static void main(String[] args) {
        int taskTime = Integer.valueOf(args[2]);
        int retCnt = Integer.valueOf(args[3]);
        int evaCnt = Integer.valueOf(args[4]);
        int dispCnt = Integer.valueOf(args[5]);
        int rpsSE = Integer.valueOf(args[6]);
        int rpsPE = Integer.valueOf(args[7]);
        int sec = Integer.valueOf(args[8]);
        Log.setDebug(Integer.valueOf(args[9]));
        
        MrClient client;
        try {
            client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HproseWrapper(), 20000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter("log/HproseEva@" + EvaClient.currentTime() + ".log"));
            EvaClient evaClient = new EvaClient(client, System.out, writer);
            evaClient.setTaskTime(taskTime);
            evaClient.setReturnCount(retCnt);
            evaClient.setEvaCount(evaCnt);
            evaClient.setThreadCount(dispCnt);
            
            evaClient.testSR("SequentialExecute");
            evaClient.testSR("ParallelExecute");
            
            for (int i = 0; i < 3; ++i) {
                System.out.println("\n# " + i);
                writer.write("\n# " + i + "\n");
                evaClient.testPR("SequentialExecute", rpsSE, sec);
            }
            for (int i = 0; i < 3; ++i) {
                System.out.println("\n# " + i);
                writer.write("\n# " + i + "\n");
                evaClient.testPR("ParallelExecute", rpsPE, sec);
            }
            
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
