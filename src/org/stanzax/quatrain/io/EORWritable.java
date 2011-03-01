/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author basicthinker
 *
 */
public class EORWritable implements Writable {

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#getValue()
     */
    @Override
    public Object getValue() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        return;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        return;
    }

}
