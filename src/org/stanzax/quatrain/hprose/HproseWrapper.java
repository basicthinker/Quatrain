/**
 * 
 */
package org.stanzax.quatrain.hprose;

import java.lang.reflect.Type;

import org.stanzax.quatrain.io.Writable;
import org.stanzax.quatrain.io.WritableWrapper;

/**
 * @author basicthinker
 *
 */
public class HproseWrapper extends WritableWrapper {

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#newInstance(java.lang.reflect.Type)
     */
    @Override
    public Writable newInstance(Type classType) {
        return new HproseWritable(null);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(int)
     */
    @Override
    public Writable valueOf(int value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(long)
     */
    @Override
    public Writable valueOf(long value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(boolean)
     */
    @Override
    public Writable valueOf(boolean value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(byte)
     */
    @Override
    public Writable valueOf(byte value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(double)
     */
    @Override
    public Writable valueOf(double value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(float)
     */
    @Override
    public Writable valueOf(float value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.lang.String)
     */
    @Override
    public Writable valueOf(String value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.lang.Object)
     */
    @Override
    public Writable valueOf(Object value) {
        return new HproseWritable(value);
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.lang.Object[])
     */
    @Override
    public Writable valueOf(Object[] value) {
        return new HproseWritable(value);
    }

}
