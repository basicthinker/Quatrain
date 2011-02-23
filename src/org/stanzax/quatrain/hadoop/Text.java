/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import org.stanzax.quatrain.io.Writable;

/**
 * @author basicthinker
 *
 */
public class Text extends org.apache.hadoop.io.Text implements Writable {

	public Text(String value) {
		super(value);
	}

	public Text() {
		super();
	}

}
