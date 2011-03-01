/**
 * 
 */
package org.stanzax.quatrain.hadoop;

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
        return get();
    }

}
