/**
 * 
 */
package org.stanzax.quatrain.io;

/**
 * ByteArrayOutputStream that give direct access to its backing array
 * 
 * @author basicthinker
 * 
 */
public class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {

    public ByteArrayOutputStream() {
        super();
    }

    /**
     * Specify initial array size to optimize reallocation
     * @param size the initial length of backing array
     * */
    public ByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getByteArray() {
        return buf;
    }
}
