/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class ObjectWritable extends org.apache.hadoop.io.ObjectWritable
        implements Writable {

    /**
	 * 
	 */
    public ObjectWritable() {
        super();
    }

    /**
     * @param instance
     */
    public ObjectWritable(Object instance) {
        super(instance);
    }

    /**
     * @param declaredClass
     * @param instance
     */
    public ObjectWritable(Class<?> declaredClass, Object instance) {
        super(declaredClass, instance);
    }

    @Override
    public Object getValue() {
        return super.get();
    }

    @Override
    public void setValue(Object value) {
        super.set(value);
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
        super.readFields(in);
    }

    @Override
    public void write(DataOutputStream out) throws IOException {
        super.write(out);
    }

}
