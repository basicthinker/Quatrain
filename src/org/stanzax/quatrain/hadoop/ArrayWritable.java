/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class ArrayWritable extends org.apache.hadoop.io.ArrayWritable implements
        Writable {

    /**
     * @param valueClass
     */
    public ArrayWritable(
            Class<? extends org.apache.hadoop.io.Writable> valueClass) {
        super(valueClass);
    }

    /**
     * @param arg0
     */
    public ArrayWritable(String[] arg0) {
        super(arg0);
    }

    /**
     * @param valueClass
     * @param values
     */
    public ArrayWritable(
            Class<? extends org.apache.hadoop.io.Writable> valueClass,
            org.apache.hadoop.io.Writable[] values) {
        super(valueClass, values);
    }

    @Override
    public Object getValue() {
        return get();
    }

}
