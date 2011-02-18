/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;

/**
 * @author basicthinker
 * 
 */
public interface DataOutput extends java.io.DataOutput {

    void writeObject(Writable object) throws IOException;

}
