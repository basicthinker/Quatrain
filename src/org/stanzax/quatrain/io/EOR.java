/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author basicthinker
 *
 */
public class EOR implements Writable {

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
    public void readFields(DataInputStream in) throws IOException {
        return;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutputStream out) throws IOException {
        return;
    }

    @Override
    public void setValue(Object value) {
        return;
    }

    private static final long serialVersionUID = 1L;
}
