/**
 * 
 */
package org.stanzax.quatrain.io;

import java.lang.reflect.Type;

/**
 * @author basicthinker
 * 
 */
public abstract class WritableWrapper {
    
    public abstract Writable valueOf(int intValue);

    public abstract Writable valueOf(long longValue);

    public abstract Writable valueOf(boolean boolValue);

    public abstract Writable valueOf(byte charValue);

    public abstract Writable valueOf(double doubleValue);

    public abstract Writable valueOf(float floatValue);

    public abstract Writable valueOf(String stringValue);
    
    public Writable valueOf(EOR eor) {
        return new EOR();
    }

    public abstract Writable valueOf(Object objValue);

    public abstract Writable valueOf(Object[] listValue);

    public abstract Writable newInstance(Type classType);
    
}
