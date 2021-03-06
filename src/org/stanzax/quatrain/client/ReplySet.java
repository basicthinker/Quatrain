/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;

/**
 * Container of multiple returns
 */
public class ReplySet {
    
    public ReplySet(Writable type, long timeout) {
        this.returnType = type;
        this.timeout = timeout;
    }
    
    public void close() {
        if (callID == 0) return;
        waiting.remove(callID);
    }
    
    public void finalize() throws Throwable {
        super.finalize();
        close();
    }
    /**
     * Register for awaiting reply. 
     * Invoked before sending request to guarantee no reply omitted.
     */
    public void register(int callID) {
        this.callID = callID;
        waiting.put(callID, this);
        if (Log.DEBUG) Log.action("New result set is registered, .current total #", waiting.size());
    }
    
    public boolean timedOut() {
        return timedOut;
    }
    
    public boolean hasError() {
        return errors.size() > 0;
    }
    
    /** Whether this set contains all expected results including possible errors */
    public boolean isPartial() {
        return !done;
    }
    
    public void putError(String errorMessage) {
        errors.add(errorMessage);
    }
    
    public String errorMessage() {
        StringBuilder errorInfo = new StringBuilder();
        for (String error : errors) {
            errorInfo.append("[QUATRAIN]");
            errorInfo.append(error);
        }
        return errorInfo.toString();
    }
    
    /**
     * Test whether new element it ready in non-blocking manner
     * */
    public synchronized boolean isReady() {
        return buffer != null 
            || (done && replyQueue.size() > 1)
            || (!done && replyQueue.size() > 0);
    }
    
    /**
     * Tell whether nextElement() would safely return an element
     * */
    public boolean hasMore() {
        if (isReady()) {
            return true;
        } else return tryNext();
    }

    public Object nextElement() {
        if (buffer != null || tryNext()) {
            try {
                return buffer;
            } finally {
                buffer = null;
            }
        } else return null;
    }
    
    /** 
     * Input should only contain data entries
     * @return true if any error happens, false otherwise.
    */
    public boolean putData(DataInputStream dataIn) {
        if (!timedOut) { // otherwise no additional elements would be retrieved,
                         // so there would be no meaning to put in new data
            try {
                while (dataIn.available() > 0) {
                    returnType.readFields(dataIn);
                    replyQueue.add(returnType.getValue());
                    if (Log.DEBUG) Log.action("[ReplySet] Call # read in data.", 
                            callID, returnType.getValue());
                }
                return false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
    
    public synchronized void putEnd() {
        replyQueue.add(new EOR());
        done = true;
        if (Log.DEBUG) Log.action("[ReplySet] Call # reaches reply end.", callID);
    }
    
    /**
     * Try to retrieve an additional element and store it in {@link#buffer}.
     * The {@link#buffer} would be overwritten, 
     * so test whether it is null before invoking this method. <br>
     * <p>This is the single point where {@link#replyQueue} is dequeued.
     * */
    private boolean tryNext() {
        if (!timedOut) try {
            Object next = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if (next == null) timedOut = true;
            else if (next instanceof EOR) next = null;
            buffer = next;
            return buffer != null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    private int callID = 0;
    private Writable returnType;
    private long timeout;
    private LinkedBlockingQueue<Object> replyQueue = new LinkedBlockingQueue<Object>();
    private Object buffer = null;
    private Vector<String> errors = new Vector<String>();
    /** Indicates whether the EOR is enqueued. */
    private volatile boolean done = false;
    /** Indicates whether additional elements should be retrieved */
    private volatile boolean timedOut = false;
    
    public static ReplySet get(int callID) {
        return waiting.get(callID);
    }
    
    private static ConcurrentHashMap<Integer, ReplySet> waiting = 
        new ConcurrentHashMap<Integer, ReplySet>();
}
