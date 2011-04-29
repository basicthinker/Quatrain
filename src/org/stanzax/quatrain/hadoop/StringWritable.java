/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 * 
 */
public class StringWritable extends org.apache.hadoop.io.Text implements Writable {

    public StringWritable(String value) {
        super(value);
    }

    public StringWritable() {
        super();
    }

    @Override
    public Object getValue() {
        return super.toString();
    }

    @Override
    public void setValue(Object value) {
        super.set((String)value);
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
        super.readFields(in);
    }

    @Override
    public void write(DataOutputStream out) throws IOException {
        super.write(out);
    }

}
