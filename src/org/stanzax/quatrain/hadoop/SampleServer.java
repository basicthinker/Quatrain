/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author basicthinker
 * 
 */
public class SampleServer extends MrServer {

    /**
     * Default configuration uses 2 times core pool size as maximum pool size
     * 
     * @param address
     *            the binded host address
     * @param port
     *            the binded port of host
     * @param handlerCount
     *            the number of handlers' core pool size
     * @param responderCount
     *            the number of responders' core pool size
     */
    public SampleServer(String address, int port, int handlerCount,
            int responderCount) throws IOException {
        super(address, port, new HadoopWrapper(), new ThreadPoolExecutor(
                handlerCount, 2 * handlerCount, 6, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()), new ThreadPoolExecutor(
                responderCount, 2 * responderCount, 6, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()));
    }

    /** Remotely called procedure */
    private void functionName() {
        int[] data = { 0, 1, 2, 3, 4, 5, 6 };
        for (int index : data) {
            new Thread(new MrWorker(data[index])).start();
        }

        try {
            // partially return
            preturn("0 arrives without awaiting -1.");
            Thread.sleep(1000); // simulates calculation or straggler
            preturn("-1 arrives without defering 0.");
        } catch (InterruptedException e) {
            // when not all preturns finish
            e.printStackTrace();
        }
    }

    /** Thread that constructs partial return(s) */
    private class MrWorker implements Runnable {

        public MrWorker(int item) {
            this.item = item;
        }

        @Override
        public void run() {
            // partially return
            preturn(item);
        }

        private int item;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            Log.setDebug(true);
            SampleServer server = new SampleServer("localhost", 3122, 3, 5);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
