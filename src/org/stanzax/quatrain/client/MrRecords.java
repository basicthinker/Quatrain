/**
 * 
 */
package org.stanzax.quatrain.client;

import java.util.Enumeration;

/**
 * Container of multiple returns
 * @param <ElementType> Type of returns
 */
public class MrRecords<ElementType> implements Enumeration<ElementType> {

	public boolean isPartial() {
		// TODO Method stub
		return true;
	}

	@Override
	public boolean hasMoreElements() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ElementType nextElement() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
