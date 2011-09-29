/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Jinglei Ren
 * This interface provides direct socket channel IO classes.
 * Generally its implementing classes transfer large volume of data
 * so that writing/reading DataOutputStream/DataInputStream(in memory) is impractical.
 * For example, file transfer protocol is suitable to under this interface.
 */
public interface DirectWritable {

    /**
     * Notice that the parameter channel is already synchronized.
     * */
    long write(OutputStream ostream) throws IOException;
    
    long read(InputStream istream) throws IOException;
    
    /**
     * Besides socket IO, some necessary data can be set as utility or tag.
     * */
    void setValue(Object value);
    
    /**
     * Besides socket IO, this interface provides the method to retrieve some extra data.
     * The returned value is put into ReplySet after each invocation of {@link #read(InputStream)}.
     * */
    Object getValue();
    
}
