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

    public EvaClient(MrClient client, PrintStream printer, BufferedWriter writer) {
        this.client = client;
        this.printer = printer;
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
    
    public void setThreadCount(int num) {
        dispatcherCount = num;
    }
    
    public void setPrintStream(PrintStream printer) {
        this.printer = printer;
    }
    
    public void setFileWriter(BufferedWriter writer) {
        this.writer = writer;
    }
    
    /* Sequential Requests */
    public void testSR(String method) throws IOException {
        printer.println("--------------------");
        printer.print(method + " SR\n" + getConfig());
        printer.println("--------------------");
        
        // Warm up
        ReplySet returns = client.invoke(String.class, method, taskTime, 3);
        while (returns.nextElement() != null);
        returns.close();
        
        /* Evaluate each number of returns */
        for (int retCnt = 1; retCnt <= returnCount; ++retCnt) {
            double costTime = 0;
            for (int i = 0; i < evaCount; ++i) {
                costTime += evaInvoke(method, retCnt);
            }
            printer.println(retCnt + " returns' average latency (ms) = " + costTime / evaCount);
        }
        printer.println();
    }
    
    /* Parallel Requests */
    public void testPR(String method, int rps, int sec) throws IOException {
        printer.println("--------------------");
        printer.print(method + " PR\n" + getConfig());
        printer.println("--------------------");
        
        writer.write("--------------------\n");
        writer.write(method + " PR\n" + getConfig());
        writer.write("--------------------\n");
        
        final int interval = 1000 / rps * dispatcherCount;
        if (interval < 3) { // prevent too short interval
            writer.write("Illegal interval: " + interval);
            writer.flush();
            return;
        }
        
        // Warm up
        ReplySet returns = client.invoke(String.class, method, taskTime, 3);
        while (returns.nextElement() != null);
        returns.close();
        
        /* Evaluate each number of returns */
        for (int retCnt = 1; retCnt <= returnCount; ++retCnt) {
            // Use several dispatchers to periodically trigger parallel requests
            for (int i = 0; i < dispatcherCount; ++i) {
                new Thread(new Dispatcher(method, retCnt, interval, rps * sec / dispatcherCount)).start();
            }
            try {
                printer.print(retCnt + " returns' sampling:");
                Thread.sleep(1000);
                int actCnt = Thread.currentThread().getThreadGroup().activeCount();
                while (actCnt > 2) {
                    printer.print(" " + actCnt);
                    Thread.sleep(1000);
                    actCnt = Thread.currentThread().getThreadGroup().activeCount();
                } // wait until all triggered threads finish
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            printer.println();
            writer.flush();
        }    
    }
    
    /**
     * Use predefined parameters to invoke the method of specific number of returns.
     * @return cost time
     */
    private double evaInvoke(String method, int retCnt) {
        int count = 0;
        double costTime = 0;
        long callTime = System.currentTimeMillis(); // accurate begin time of call
        ReplySet returns = client.invoke(String.class, method, taskTime, retCnt);

        String returnValue = (String) returns.nextElement();
        while (returnValue != null) {
            if (!returnValue.equals("3.1415926")) 
                printer.println("WRONG STRING : " + returnValue);
            ++count;
            costTime += System.currentTimeMillis() - callTime; // sum each reply latency
            returnValue = (String) returns.nextElement();
        }
        returns.close();
        if (count != retCnt) { 
            printer.println("WRONG COUNT! # expected returns != # actual returns .timed-out : " 
                    + retCnt + " : " + count + " : " + returns.timedOut());
            return Double.MAX_VALUE;
        } else return costTime / count;
    }
    
    public String getConfig() {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("Client Configuration @").append(currentTime()).append("\n");
        strBuf.append("Task time = ").append(taskTime).append("\n");
        strBuf.append("Max return count = ").append(returnCount).append("\n");
        strBuf.append("Evaluation count = ").append(evaCount).append("\n");
        return strBuf.toString();
    }
    
    static String currentTime() {
        SimpleDateFormat formatter = 
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(new Date());
    }
    
    private MrClient client;
    private int taskTime = 0;
    private int returnCount = 0;
    private int evaCount = 0;
    private int dispatcherCount = 0;
    private PrintStream printer;
    private BufferedWriter writer;
    
    class Dispatcher implements Runnable {

        public Dispatcher(String method, int retCnt, int interval, int reqCnt) {
            this.method = method;
            this.returnCount = retCnt;
            this.interval = interval;
            this.requestCount = reqCnt;
        }
        
        @Override
        public void run() {
            for (int i = 0; i < requestCount; ++i) {
                new Thread(new RequestThread(method, returnCount)).start();
                try {
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
        private int interval = 0;
        private int requestCount = 0;
        private String method = null;
        private int returnCount = 0;
    }
    
    class RequestThread implements Runnable {

        public RequestThread(String method, int retCnt) {
            methodName = method;
            returnCount = retCnt;
        }
        
        @Override
        public void run() {
            try {
                writer.write(System.currentTimeMillis() + "\tR\t" + returnCount + "\n"); // request time
                double costTime = evaInvoke(methodName, returnCount);
                writer.write(costTime + "\tL\t" + returnCount + "\n"); // latency time
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        private String methodName = null;
        private int returnCount = 0;
    }
}
