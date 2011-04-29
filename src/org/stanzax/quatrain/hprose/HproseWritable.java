/**
 * 
 */
package org.stanzax.quatrain.hprose;

import hprose.io.HproseReader;
import hprose.io.HproseWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 *
 */
public class HproseWritable implements Writable {
    
    public HproseWritable(Object value) {
        this.value = value;
    }
    
    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#getValue()
     */
    @Override
    public Object getValue() {
        return value;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#setValue(java.lang.Object)
     */
    @Override
    public void setValue(Object value) {
        this.value = value;
    }
    
    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInputStream in) throws IOException {
        value = new HproseReader(in).unserialize();
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutputStream out) throws IOException {
        new HproseWriter(out).serialize(value);
    }
    
    private Object value;

}
