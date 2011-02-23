/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.util.List;
import java.util.Map;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 *
 */
public class HadoopWrapper implements org.stanzax.quatrain.io.WritableWrapper {

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#newInstance(java.lang.Class)
	 */
	@Override
	public Writable newInstance(Class classType) {
		if (classType == Integer.TYPE) return new IntWritable();
		else if (classType == Long.TYPE) return new LongWritable();
		else if (classType == Boolean.TYPE) return new BooleanWritable();
		else if (classType == Character.TYPE) return new ByteWritable();
		else if (classType == Double.TYPE) return new DoubleWritable();
		else if (classType == Float.TYPE) return new FloatWritable();
		else if (classType == String.class) return new Text();
		else throw new IllegalArgumentException("Invalid class type");
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(int)
	 */
	@Override
	public Writable valueOf(int intValue) {
		return new IntWritable(intValue);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(long)
	 */
	@Override
	public Writable valueOf(long longValue) {
		return new LongWritable(longValue);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(boolean)
	 */
	@Override
	public Writable valueOf(boolean boolValue) {
		return new BooleanWritable(boolValue);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(char)
	 */
	@Override
	public Writable valueOf(char charValue) {
		return new ByteWritable(charValue);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(double)
	 */
	@Override
	public Writable valueOf(double doubleValue) {
		return new DoubleWritable(doubleValue);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(float)
	 */
	@Override
	public Writable valueOf(float floatValue) {
		return new FloatWritable(floatValue);
	}
	
	@Override
	public Writable valueOf(String stringValue) {
		return new Text(stringValue);
	}
	
	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.lang.Object)
	 */
	@Override
	public Writable valueOf(Object objValue) {
		return new ObjectWritable();
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.util.List)
	 */
	@Override
	public Writable valueOf(List listValue) {
		return new ArrayWritable(ObjectWritable.class);
	}

	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.WritableWrapper#valueOf(java.util.Map)
	 */
	@Override
	public Writable valueOf(Map mapValue) {
		return new MapWritable();
	}

}
