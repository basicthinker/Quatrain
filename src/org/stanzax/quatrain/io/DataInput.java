/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;

/**
 * @author basicthinker
 * 
 */
public interface DataInput extends java.io.DataInput {

    void readObject(Writable object) throws IOException;

}
