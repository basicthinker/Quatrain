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
    
    public boolean hasMore() {
        if (replyQueue.size() > 0) return true;
        else if (timedOut) return false;
        else return !done;
    }

    public Object nextElement() {
        if (!timedOut) {
            try {
                Object element = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
                if (element == null) timedOut = true;
                else if (!(element instanceof EOR)) return element;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    
    public String errorMessage() {
        StringBuffer errorInfo = new StringBuffer();
        for (String error : errors) {
            errorInfo.append("[QUATRAIN]");
            errorInfo.append(error);
        }
        return errorInfo.toString();
    }
    
    /** 
     * Input should only contain data entries
     * @return true if actual data are put, false if EOF or error is encountered
    */
    public boolean putData(DataInputStream dataIn) {
        if (!timedOut) {
            try {
                if (dataIn.available() == 0) { 
                    // end of frame denoting final return
                    replyQueue.add(new EOR());
                    done = true;
                    if (Log.DEBUG) Log.action("[ReplySet] Call # reaches reply end.", callID);
                    return false;
                } else {
                    while (dataIn.available() > 0) {
                        returnType.readFields(dataIn);
                        replyQueue.add(returnType.getValue());
                        if (Log.DEBUG) Log.action("[ReplySet] Call # read in data.", 
                                callID, returnType.getValue());
                    }
                    return true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public void putError(String errorMessage) {
        if (!timedOut) {
            errors.add(errorMessage);
        } else return;
    }
    
    public void close() {
        if (callID == 0) return;
        waiting.remove(callID);
    }
    
    public void finalize() throws Throwable {
        super.finalize();
        close();
    }
    
    public static ReplySet get(int callID) {
        return waiting.get(callID);
    }
    
    private int callID = 0;
    private Writable returnType;
    private long timeout;
    private LinkedBlockingQueue<Object> replyQueue = new LinkedBlockingQueue<Object>();
    private Vector<String> errors = new Vector<String>();

    private volatile boolean done = false; // whether the EOR is enqueued.
    private volatile boolean timedOut = false;
    private static ConcurrentHashMap<Integer, ReplySet> waiting = 
        new ConcurrentHashMap<Integer, ReplySet>();
}
