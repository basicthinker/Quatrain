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

import org.stanzax.quatrain.hadoop.BooleanWritable;
import org.stanzax.quatrain.hadoop.StringWritable;
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
        if (Log.debug) Log.debug("New result set registered, .current # waiting", waiting.size());
    }
    
    public boolean hasError() {
        return errors.size() > 0;
    }
    
    /** Whether this set contains all expected results including possible errors */
    public boolean isPartial() {
        return isPartial;
    }
    
    public boolean hasMore() {
        if (!replyQueue.isEmpty()) {
            return true;
        } else if (isTimedOut) {
            return false;
        } else if (!isPartial) {
            return false;
        } else {
            Object element = nextElement();
            if (element != null) {
                replyQueue.add(element);
                return true;
            } return false;
        }
    }

    public Object nextElement() {
        try {
            Object element = replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if (element == null) isTimedOut = true;
            return element;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
    
    /** Input should begin with error flag */
    public void putData(DataInputStream dataIn) {
        if (!isTimedOut) {
            try {
                BooleanWritable errorFlag = new BooleanWritable();
                errorFlag.readFields(dataIn);
                if (errorFlag.get()) {
                    StringWritable errorMessage = new StringWritable();
                    errorMessage.readFields(dataIn);
                    errors.add(errorMessage.toString());
                } else if (dataIn.available() == 0) { 
                    // end of frame denoting final return
                    isPartial = false;
                } else {
                    while (dataIn.available() > 0) {
                        returnType.readFields(dataIn);
                        replyQueue.offer(returnType.getValue(),
                                timeout, TimeUnit.MILLISECONDS);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
    private volatile boolean isPartial = true;
    private volatile boolean isTimedOut = false;
    private long beginTime = System.currentTimeMillis();
    
    private static ConcurrentHashMap<Long, ResultSet> waiting = 
        new ConcurrentHashMap<Long, ResultSet>();
}
