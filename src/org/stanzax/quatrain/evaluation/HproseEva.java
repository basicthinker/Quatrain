/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
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
        int repeatCnt = Integer.valueOf(args[4]);
        int dispCnt = Integer.valueOf(args[5]);
        int rpsSE = Integer.valueOf(args[6]);
        int rpsPE = Integer.valueOf(args[7]);
        int sec = Integer.valueOf(args[8]);
        Log.setDebug(Integer.valueOf(args[9]));
        
        MrClient client;
        try {
            client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), new HproseWrapper(), taskTime * 100);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            StringBuilder fileName = new StringBuilder().append("log");
            fileName.append(System.getProperty("file.separator"));
            fileName.append("hprose-t").append(taskTime).append("-r").append(retCnt);
            fileName.append(repeatCnt > 0 ? "-SR" : "");
            fileName.append(dispCnt > 0 ? "-PR" : "");
            fileName.append("-p").append(args[1]).append('@');
            fileName.append((int)System.currentTimeMillis());
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(fileName.toString()));
            
            PrintStream printer = new PrintStream(new File("check.log"));
            
            EvaClient evaClient = new EvaClient(client, printer, writer);
            evaClient.setTaskTime(taskTime);
            evaClient.setReturnCount(retCnt);
            evaClient.setThreadCount(dispCnt);
            
            if (repeatCnt != 0) {
                evaClient.testSR("SequentialExecute", repeatCnt);
                evaClient.testSR("ParallelExecute", repeatCnt);
            }
            
            if (dispCnt != 0) {
                for (int i = 1; i <= 3; ++i) {
                    printer.println("\n# " + i);
                    System.out.println("Testing SequentialExecute...(" + i + "/3)");
                    evaClient.testPR("SequentialExecute", rpsSE, sec);
                    System.out.println("Testing ParallelExecute...(" + i + "/3)");
                    evaClient.testPR("ParallelExecute", rpsPE, sec);
                }
            }
            
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
