/**
 * 
 */
package org.stanzax.quatrain.io;

import java.util.List;
import java.util.Map;


/**
 * @author basicthinker
 *
 */
public interface WritableWrapper {
	public abstract Writable valueOf(int intValue);
	public abstract Writable valueOf(long longValue);
	public abstract Writable valueOf(boolean boolValue);
	public abstract Writable valueOf(char charValue);
	public abstract Writable valueOf(double doubleValue);
	public abstract Writable valueOf(float floatValue);
	public abstract Writable valueOf(String stringValue);
	public abstract Writable valueOf(Object objValue);
	public abstract Writable valueOf(List listValue);
	public abstract Writable valueOf(Map mapValue);
	
	public abstract Writable newInstance(Class classType);
}
