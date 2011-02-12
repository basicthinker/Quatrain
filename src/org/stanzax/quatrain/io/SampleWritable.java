/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author basicthinker
 *
 */
public class SampleWritable implements Writable {

	public SampleWritable(String[] content) {
		this.content = content;
	}
	
	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(content.length);
		for (String element : content) {
			out.writeUTF(element);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.stanzax.quatrain.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		content = new String[count];
		for (int i = 0; i < count; ++i) {
			content[i] = in.readUTF();
		}
	}

	private String[] content;
	
}
