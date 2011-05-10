/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.lang.reflect.Type;
import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class HadoopWrapper implements org.stanzax.quatrain.io.WritableWrapper {

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#newInstance(java.lang.Class)
     */
    @Override
    public Writable newInstance(Type classType) {
        // most primitive types
        if (classType == Integer.TYPE || classType == Integer.class)
            return new IntWritable();
        else if (classType == Long.TYPE || classType == Long.class)
            return new LongWritable();
        else if (classType == Boolean.TYPE || classType == Boolean.class)
            return new BooleanWritable();
        else if (classType == Double.TYPE || classType == Double.class)
            return new DoubleWritable();
        else if (classType == String.class)
            return new StringWritable();
        else if (classType == Float.TYPE || classType == Float.class)
            return new FloatWritable();
        else if (classType == Character.TYPE || classType == Character.class)
            return new ByteWritable();
        else
            throw new IllegalArgumentException("Invalid class type " + classType);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.lang.Object)
     */
    @Override
    public Writable valueOf(Object object) {
        if (object instanceof Integer) {
            return new IntWritable((Integer)object);
        } else if (object instanceof Long) {
            return new LongWritable((Long)object);
        } else if (object instanceof Boolean) {
            return new BooleanWritable((Boolean)object);
        } else if (object instanceof Byte) {
            return new ByteWritable((Byte)object);
        } else if (object instanceof Double) {
            return new DoubleWritable((Double)object);
        } else if (object instanceof Float) {
            return new FloatWritable((Float)object);
        } else if (object instanceof String) {
            return new StringWritable((String)object);
        } else if (object instanceof Writable){
            return (Writable)object;
        } else if (object.getClass().isArray()) {
            return new ArrayWritable((Object[])object);
        } else {
            return new ObjectWritable(object);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(int)
     */
    @Override
    public Writable valueOf(int intValue) {
        return new IntWritable(intValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(long)
     */
    @Override
    public Writable valueOf(long longValue) {
        return new LongWritable(longValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(boolean)
     */
    @Override
    public Writable valueOf(boolean boolValue) {
        return new BooleanWritable(boolValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(char)
     */
    @Override
    public Writable valueOf(byte byteValue) {
        return new ByteWritable(byteValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(double)
     */
    @Override
    public Writable valueOf(double doubleValue) {
        return new DoubleWritable(doubleValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(float)
     */
    @Override
    public Writable valueOf(float floatValue) {
        return new FloatWritable(floatValue);
    }

    @Override
    public Writable valueOf(String stringValue) {
        return new StringWritable(stringValue);
    }

    @Override
    public Writable valueOf(Object[] listValue) {
        return new ArrayWritable(listValue);
    }
}
