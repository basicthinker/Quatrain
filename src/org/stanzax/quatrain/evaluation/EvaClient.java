/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.net.InetAddress;
import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ResultSet;
import org.stanzax.quatrain.hadoop.HadoopWrapper;

/**
 * @author basicthinker
 *
 */
public class EvaClient {

    /**
     * @param args
     *          args[0] Remote server address
     *          args[1] Total work time to simulate
     *          args[2] Max number of parts server divides whole task into
     *          args[3] Number of loops to execute evaluation
     */
    public static void main(String[] args) {
        MrClient client;
        try {
            client = new MrClient(InetAddress.getByName(args[0]),
                    3122, new HadoopWrapper(), 6000);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        int totalWorkTime = Integer.valueOf(args[1]);
        int maxReturnCount = Integer.valueOf(args[2]);
        int loopCount = Integer.valueOf(args[3]);
        System.out.println("Evaluation Parameters:");
        System.out.println("Total work time =\t" + totalWorkTime);
        System.out.println("Max return count =\t" + maxReturnCount);
        System.out.println("Max execution count =\t" + loopCount);
        
        long timeCost = 0;
        
        /* Sequential Requests, Sequential Execution */
        System.out.println("- SRSE - ");
        for (int returnCount = 1; returnCount <= maxReturnCount; ++returnCount) {
            timeCost = System.currentTimeMillis();
            for (int i = 0; i < loopCount; ++i) {
                ResultSet returns = client.invoke(String.class, "SequentialExecute",
                        totalWorkTime, returnCount);
                int count = 0;
                String returnValue = (String) returns.nextElement();
                while (returnValue != null) {
                    if (!returnValue.equals("3.1415926")) 
                        System.out.println("*!WRONG STRING!*");
                    ++count;
                    returnValue = (String) returns.nextElement();
                }
                if (count != returnCount) 
                    System.out.println("WRONG COUNT" + returnCount);
            }
            timeCost = System.currentTimeMillis() - timeCost;
            System.out.println("return count = " + returnCount + 
                    "\taverage latency (ms) = " + (double)timeCost / loopCount);
        }
        System.out.println();
        
        /* Sequential Request, Concurrent Respond */
        System.out.println("- SRCE - ");
        for (int returnCount = 1; returnCount <= maxReturnCount; ++returnCount) {
            timeCost = System.currentTimeMillis();
            for (int i = 0; i < loopCount; ++i) {
                ResultSet returns = client.invoke(String.class, "ConcurrentExecute",
                        totalWorkTime, returnCount);
                int count = 0;
                String returnValue = (String) returns.nextElement();
                while (returnValue != null) {
                    if (!returnValue.equals("3.1415926")) 
                        System.out.println("*!WRONG STRING!*");
                    ++count;
                    returnValue = (String) returns.nextElement();
                }
                if (count != returnCount) 
                    System.out.println("WRONG COUNT" + returnCount);
            }
            timeCost = System.currentTimeMillis() - timeCost;
            System.out.println("return count = " + returnCount + 
                    "\taverage latency (ms) = " + (double)timeCost / loopCount);
        }
    }

}
