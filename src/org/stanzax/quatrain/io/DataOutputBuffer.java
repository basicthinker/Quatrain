/**
 * 
 */
package org.stanzax.quatrain.io;

/**
 * Byte-array-based dataOutputStream that give direct access to its backing array
 * 
 * @author basicthinker
 * 
 */
public class DataOutputBuffer extends java.io.DataOutputStream {

    public DataOutputBuffer() {
        this(new ByteArrayOutputStream());
    }
    
    public DataOutputBuffer(int capacity) {
        this(new ByteArrayOutputStream(capacity));
    }
    
    private DataOutputBuffer(ByteArrayOutputStream buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    public byte[] getData() {
        return buffer.getByteArray();
    }
    
    public int getDataLength() {
        return buffer.size();
    }
    
    private ByteArrayOutputStream buffer;
    
    private static class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
        /**
    	 * 
    	 */
        public ByteArrayOutputStream() {
            super();
        }
    
        /**
         * @param size
         */
        public ByteArrayOutputStream(int size) {
            super(size);
        }
    
        public byte[] getByteArray() {
            return buf;
        }
    }
}
