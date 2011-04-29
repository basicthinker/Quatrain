/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Type;
import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 *
 */
public class ArrayWritable implements Writable {

    /**
     * 
     */
    public ArrayWritable(Object[] list) {
        this.list = list;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#getValue()
     */
    @Override
    public Object getValue() {
        return list;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#setValue(java.lang.Object)
     */
    @Override
    public void setValue(Object value) {
        list = (Object[])value;
    }
    
    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInputStream in) throws IOException {
        Type elementType = list.getClass().getComponentType();
        Writable elementWritable = wrapper.valueOf(elementType);
        while (true) {
            try {
                elementWritable.readFields(in);
            } catch (EOFException e) {
                break;
            }
        }
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutputStream out) throws IOException {
        Type elementType = list.getClass().getComponentType();
        Writable elementWritable = wrapper.newInstance(elementType);
        for (Object element : list) {
            elementWritable.setValue(element);
            elementWritable.write(out);
        }
    }
    
    private static HadoopWrapper wrapper = new HadoopWrapper();
    private Object[] list;
}
