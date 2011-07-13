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
        maxReturnNum = count;
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
    
    /**
     * Guarantee that at least three successive RPCs reply in normal delay,
     * to avoid timed-out replies of last peak that may affect next evaluation.
     * */
    void avoidPeak(String method) {
        while (true) {
            int cnt = 0;
            for (int i = 0; i < 3; ++i) {
                if (evaInvoke(method, 1) < taskTime * 1.05)
                    ++cnt;
            }
            if (cnt == 3) return;
            else if (cnt > 0) continue;
            else try {
                long wait = taskTime * 10;
                Thread.sleep(wait < 30000 ? wait : 30000);
            } catch (InterruptedException e) {
                continue;
            }
        }
    }
    
    /* Sequential Requests */
    public void testSR(String method, final int repeatCnt) throws IOException {
        if (taskTime == 0) return;
        
        printer.print("########################################\n# ");
        printer.println(method + " SR for Remote " + client.getRemoteSocketAddress().toString());
        printer.println("#  Repeat Count = " + repeatCnt);
        printer.print(getReadableConfig());
        printer.println("# Return number\tLatency\tSTDEV\tBegin time\tEnd time");
        
        // Warm up
        avoidPeak(method);
        
<<<<<<< HEAD
        for (int retCnt = 1; retCnt <= maxReturnNum; ++retCnt) {
            /* Evaluate each number of returns */
            double costTime = 0;
            double totalTime = 0;
            double squareTime = 0;
            partialCount = 0;
            long beginTime = System.currentTimeMillis();
            for (int i = 0; i < repeatCnt; ++i) {
                costTime = evaInvoke(method, retCnt);
                // System.out.print(costTime + "\t");
                totalTime += costTime;
                squareTime += costTime * costTime;
            }
            long endTime = System.currentTimeMillis();
            double average = totalTime / repeatCnt;
            double variance = (squareTime + average * (repeatCnt * average - 2 * totalTime)) / (repeatCnt - 1);
            if (variance < 0 && variance > -0.00001) variance = 0;
            double stdev = Math.sqrt(variance);
            StringBuilder record = new StringBuilder();
            record.append(retCnt).append('\t');
            record.append(average).append('\t').append(stdev).append('\t');
            record.append(beginTime).append('\t').append(endTime).append('\n');
            printer.print(record.toString());
            if (partialCount > 0) printer.println("\t# partial returns = " + partialCount);
        }
        printer.println();
    }
    
    /* Parallel Requests */
    public void testPR(String method, final int rps, final int sec) throws IOException {
        if (taskTime == 0 || dispatcherCount == 0 || rps == 0 || sec == 0) return;
        
        printer.print("########################################\n# ");
        printer.println(method + " PR for Remote " + client.getRemoteSocketAddress().toString());
        printer.println("#  Request/second = " + rps + "\tSeconds = " + sec);
        printer.print(getReadableConfig());
        printer.println("########################################");
        
        writer.write("-1\t" + method);
        writer.write("\t" + taskTime);
        writer.write("\t" + maxReturnNum +"\n");
        
        final int interval = 1000 * dispatcherCount / rps ;
        if (interval < 3) { // prevent too short interval
            printer.println("Illegal interval: " + interval);
            writer.flush();
            return;
        }
        
        // Warm up and prevent previous impact
        avoidPeak(method);
        
        /* Evaluate each number of returns */
        for (int retCnt = 1; retCnt <= maxReturnNum; ++retCnt) {
            // Use several dispatchers to periodically trigger parallel requests
            partialCount = 0;
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
            if (partialCount > 0) {
                printer.println("\t# partial returns = " + partialCount);
                avoidPeak(method);
            }
            writer.flush();
        }    
    }
    
    /**
     * Use predefined parameters to invoke the method of specific number of returns.
     * This method may increase the value of partialCount (partial count),
     * so set it zero before the evaluated invocations to avoid side effects of previous ones.
     * @return cost time
     */
    private double evaInvoke(String method, int retCnt) {
        int count = 0;
        double costTime = 0;
        long callTime = System.currentTimeMillis(); // accurate begin time of call
        ReplySet returns = client.invoke(Double.class, method, taskTime, retCnt);

        Double returnValue = (Double) returns.nextElement();
        while (returnValue != null) {
            if (returnValue != Math.PI) 
                printer.println("WRONG REPLY : " + returnValue);
            ++count;
            costTime += System.currentTimeMillis() - callTime; // sum each reply latency
            returnValue = (Double) returns.nextElement();
        }
        returns.close();
        if (count != taskTime) { // expected number of replyQueue equals taskTime
            if (returns.timedOut()) {
                ++partialCount;
                return System.currentTimeMillis() - callTime; // use the last reply's time as the average
            } else {
                printer.println("WRONG COUNT for # returns! # expected != # actual replyQueue : " 
                        + retCnt + " : " + taskTime + " : " + count);
                return Double.MAX_VALUE; // regarded invalid
            }
        } else return costTime / count;
    }
    
    public String getReadableConfig() {
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("# Client Configuration @").append(currentTime()).append("\n");
        strBuf.append("#  Task time = ").append(taskTime).append("\n");
        strBuf.append("#  Max return count = ").append(maxReturnNum).append("\n");
        return strBuf.toString();
    }
    
    static String currentTime() {
        SimpleDateFormat formatter = 
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(new Date());
    }
    
    private MrClient client;
    private int taskTime = 0;
    private int maxReturnNum = 0;

    private int dispatcherCount = 0;
    private PrintStream printer;
    private BufferedWriter writer;
    /* To approximate the number of partial returns due to timeout */
    private volatile long partialCount = 0;
    
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
