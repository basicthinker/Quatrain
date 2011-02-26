/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class MapWritable extends org.apache.hadoop.io.MapWritable implements
        Writable {

    /**
	 * 
	 */
    public MapWritable() {
        super();
    }

    /**
     * @param other
     */
    public MapWritable(org.apache.hadoop.io.MapWritable other) {
        super(other);
    }

}
