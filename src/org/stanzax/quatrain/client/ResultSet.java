/**
 * 
 */
package org.stanzax.quatrain.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.stanzax.quatrain.hadoop.BooleanWritable;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.Writable;

/**
 * Container of multiple returns
 * 
 * @param <ElementType>
 *            Type of returns
 */
public class ResultSet {
    
    public ResultSet(long callID, Writable returnType, long timeout) {
        this.callID = callID;
        this.returnType = returnType;
        this.timeout = timeout;
        this.iterator = results.iterator();
        beginTime = System.currentTimeMillis();
        waiting.put(callID, this);
    }

    public boolean hasError() {
        return errors.size() > 0;
    }
    
    /** Whether this set contains all expected results including possible errors */
    public boolean isPartial() {
        return isPartial;
    }
    
    public synchronized boolean hasMore() {
        if (iterator.hasNext()) {
            return true;
        } else if (isTimedOut) {
            return false;
        } else if (isPartial) {
            try {
                if (Log.debug) Log.debug("Begin awaiting replies from .callID...", callID);
                wait(timeout);
                if (Log.debug) Log.debug("Finish awaiting replies from .callID for .time",
                        callID, System.currentTimeMillis() - beginTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } 
            if (iterator.hasNext()) return true;
            else if (System.currentTimeMillis() - beginTime > timeout) {
                isTimedOut = true;
                return false;
            } else return hasMore();
        } else return false;
    }

    public Object nextElement() {
        if (hasMore()) {
            return iterator.next();
        } else return null;
    }
    
    public Vector<Object> resultSet() {
        return results;
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
        Log.info("Read in data.");
        if (!isTimedOut) {
            try {
                BooleanWritable errorFlag = new BooleanWritable();
                errorFlag.readFields(dataIn);
                if (errorFlag.get()) {
                    // TODO Deal with errors
                } else {
                    while (dataIn.available() > 0) {
                        returnType.readFields(dataIn);
                        results.add(returnType.getValue());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else return;
    }

    public void close() {
        waiting.remove(callID);
    }
    
    public void finalize() throws Throwable {
        super.finalize();
        close();
    }
    
    public static ResultSet get(long callID) {
        return waiting.get(callID);
    }
    
    private long callID;
    private Vector<Object> results = new Vector<Object>();
    private Vector<String> errors = new Vector<String>();
    private Iterator<Object> iterator;
    /** Not holding value, only for deserialization */
    private Writable returnType;
    private volatile boolean isPartial = true;
    private long beginTime;
    private long timeout;
    private boolean isTimedOut = false;
    
    private static ConcurrentHashMap<Long, ResultSet> waiting = 
        new ConcurrentHashMap<Long, ResultSet>();
}
