/**
 * 
 */
package org.stanzax.quatrain.io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * @author basicthinker
 *
 */
public class Array implements Writable {

    /**
     * 
     */
    public Array(Class<? extends Writable> valueType, Writable[] list) {
    	this.type = valueType;
        this.list = list;
    }

    public Object getValue() {
        return list;
    }

    public void setValue(Object value) {
        list = (Writable[])value;
    }
    
    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        Writable elementWritable = WritableFactories.newInstance(type);
        ArrayList<Writable> array = new ArrayList<Writable>();
        while (true) {
            try {
                elementWritable.readFields(in);
                array.add(elementWritable);
            } catch (EOFException e) {
                break;
            }
        }
        list = (Writable[]) array.toArray();
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        for (Writable element : list) {
            element.write(out);
        }
    }

    private Class<? extends Writable> type;
    private Writable[] list;
}
