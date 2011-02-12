/**
 * 
 */
package org.stanzax.quatrain.server;

import java.util.UUID;

/**
 * @author basicthinker
 *
 */
public class MrServer {

	public MrServer(String address, int port) {
		
	}
	
	public void start() {
		
	}
	
	public void stop() {
		
	}
	
	/**
	 * Returns partical results
	 * @param value the partical return value
	 */
	protected void preturn(Object value) {
		// TODO Method stub
		UUID requestID = REQUEST_ID.get();
	}
	
	protected static final InheritableThreadLocal<UUID> REQUEST_ID = new InheritableThreadLocal<UUID>();

	protected class Thread extends java.lang.Thread {
		
		public Thread() {
			super();
		}
		
		public Thread(Runnable target) {
			super(target);
		}
		
		public Thread(String name) {
			super(name);
		}
		
	}
}

