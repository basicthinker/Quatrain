/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.stanzax.quatrain.io.EOR;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;

/**
 * Container of multiple returns
 * 
 * @param <ElementType>
 *            Type of returns
 */
public class ResultSet {
    
    public ResultSet(Writable type, long timeout) {
        this.returnType = type;
        this.timeout = timeout;
    }

    /**
     * Register for awaiting reply. 
     * Invoked before sending request to guarantee no reply omitted.
     */
    public void register(long callID) {
        this.callID = callID;
        waiting.put(callID, this);
        if (Log.debug) Log.action("New result set is registered, .current total #", waiting.size());
    }
    
    public boolean hasError() {
        return errors.size() > 0;
    }
    
    /** Whether this set contains all expected results including possible errors */
    public boolean isPartial() {
        return !isDone;
    }
    
    public boolean hasMore() {
        if (buffer != null) return true;
        Object element = nextElement();
        if (element != null) {
            buffer = element;
            return true;
        } else return false;
    }

    public Object nextElement() {
        if (buffer != null) {
            Object element = buffer;
            buffer = null;
            return element;
        } else if (!isDone && !isTimedOut) {
            try {
                Object element = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
                if (element == null) isTimedOut = true;
                else if (element instanceof EOR) {
                    isDone = true;
                    return nextElement();
                }
                return element;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else return replyQueue.poll();
        if (System.currentTimeMillis() - beginTime > timeout) {
            isTimedOut = true;
            return null;
        } else return nextElement();
    }
    
    public String errorMessage() {
        StringBuffer errorInfo = new StringBuffer();
        for (String error : errors) {
            errorInfo.append("[QUATRAIN]");
            errorInfo.append(error);
        }
        return errorInfo.toString();
    }
    
    /** Input should only contain data entries */
    public void putData(DataInputStream dataIn) {
        if (!isTimedOut) {
            try {
                if (dataIn.available() == 0) { 
                    // end of frame denoting final return
                    replyQueue.add(new EOR());
                    if (Log.debug) Log.action("Result set for call # encounters reply end.", callID);
                } else while (dataIn.available() > 0) {
                    returnType.readFields(dataIn);
                    replyQueue.add(returnType.getValue());
                    if (Log.debug) Log.action("Result set for call # read in data.", 
                            callID, returnType.getValue().toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else return;
    }

    public void putError(String errorMessage) {
        if (!isTimedOut) {
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
    
    public static ResultSet get(long callID) {
        return waiting.get(callID);
    }
    
    public static Map<Long, ResultSet> getAll() {
        return waiting;
    }
    
    private long callID = 0;
    private Writable returnType;
    private long timeout;
    private LinkedBlockingQueue<Object> replyQueue = new LinkedBlockingQueue<Object>();
    private Vector<String> errors = new Vector<String>();
    /** Not holding value, only for deserialization */
    private volatile boolean isDone = false;
    private volatile boolean isTimedOut = false;
    private long beginTime = System.currentTimeMillis();
    private Object buffer = null;
    private static ConcurrentHashMap<Long, ResultSet> waiting = 
        new ConcurrentHashMap<Long, ResultSet>();
}
