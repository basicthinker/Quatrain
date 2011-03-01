/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class StringWritable extends org.apache.hadoop.io.Text implements Writable {

    public StringWritable(String value) {
        super(value);
    }

    public StringWritable() {
        super();
    }

    @Override
    public Object getValue() {
        return toString();
    }

}
