/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ReplySet;

/**
 * @author basicthinker
 *
 */
public class EvaClient {

    public EvaClient(MrClient client, BufferedWriter writer) {
        this.client = client;
        this.writer = writer;
    }
    
    public void setTaskTime(int time) {
        taskTime = time;
    }
    
    public void setReturnCount(int count) {
        returnCount = count;
    }
    
    public void setEvaCount(int count) {
        evaCount = count;
    }
    
    public void setPrintStream(PrintStream printer) {
        this.printer = printer;
    }
    
    public void setFileWriter(BufferedWriter writer) {
        this.writer = writer;
    }
    
    /* Sequential Requests, Sequential Execution */
    public void testSRSE() throws IOException {
        printer.println("- Printer for SRSE @ " + currentTime());
        writer.write("- Writer for SRSE @ " + currentTime() + "\n");

        // Warm up
        ReplySet returns = client.invoke(String.class, "SequentialExecute",
                taskTime, 1);
        returns.close();
        
        for (int retCnt = 1; retCnt <= returnCount; ++retCnt) {
            long callTime, costTime = 0;
            for (int i = 0; i < evaCount; ++i) {
                writer.write(String.valueOf(System.currentTimeMillis()) + "\n");
                callTime = System.currentTimeMillis(); //begin of each call
                returns = client.invoke(String.class, "SequentialExecute",
                        taskTime, retCnt);
                
                int count = 0;
                String returnValue = (String) returns.nextElement();
                while (returnValue != null) {
                    if (!returnValue.equals("3.1415926")) 
                        printer.println("WRONG STRING : " + returnValue);
                    ++count;
                    costTime += System.currentTimeMillis() - callTime; // sum each reply latency
                    returnValue = (String) returns.nextElement();
                }
                if (count != retCnt) 
                    printer.println("WRONG COUNT for # expected returns != # actual returns : " 
                            + retCnt + " : " + count);
                returns.close();
            }
            printer.println("return count = " + retCnt + 
                    "\taverage latency (ms) = " + (double)costTime / evaCount / retCnt);
        } // for each return count
        printer.println();
    }
    
    /* Sequential Request, Parallel Execution */
    public void testSRPE() throws IOException {
        printer.println("- Printer for SRPE @ " + currentTime());
        writer.write("- Writer for SRPE @ " + currentTime() + "\n");

        // Warm up
        ReplySet returns = client.invoke(String.class, "ConcurrentExecute",
                taskTime, 1);
        returns.close();
        
        for (int retCnt = 1; retCnt <= returnCount; ++retCnt) {
            long callTime, costTime = 0;
            for (int i = 0; i < evaCount; ++i) {
                writer.write(String.valueOf(System.currentTimeMillis()) + "\n");
                callTime = System.currentTimeMillis(); //begin of each call
                returns = client.invoke(String.class, "ConcurrentExecute",
                        taskTime, retCnt);

                int count = 0;
                String returnValue = (String) returns.nextElement();
                while (returnValue != null) {
                    if (!returnValue.equals("3.1415926")) 
                        printer.println("WRONG STRING : " + returnValue);
                    ++count;
                    costTime += System.currentTimeMillis() - callTime; // sum each reply latency
                    returnValue = (String) returns.nextElement();
                }
                if (count != retCnt) 
                    printer.println("WRONG COUNT for # expected returns != # actual returns : " 
                            + retCnt + " : " + count);
                returns.close();
            }
            printer.println("return count = " + retCnt + 
                    "\taverage latency (ms) = " + (double)costTime / evaCount / retCnt);
        }
    }
    
    
    
    static String currentTime() {
        SimpleDateFormat formatter = 
            new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        return formatter.format(new Date());
    }
    
    private MrClient client;
    private int taskTime = 0;
    private int returnCount = 0;
    private int evaCount = 0;
    private PrintStream printer = System.out;
    private BufferedWriter writer;
}
