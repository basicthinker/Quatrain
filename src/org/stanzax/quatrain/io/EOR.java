/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author basicthinker
 *
 */
public class EOR implements Writable {

	@Override
	public void write(DataOutput out) throws IOException {
		return;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		return;
	}
}
