/**
 * 
 */
package org.stanzax.quatrain.server;

/**
 * @author basicthinker
 *
 */
public class SampleServer extends MrServer {

	public SampleServer(String address, int port) {
		super(address, port);
	}

	/** Remotely called procedure */
	private void functionName() {
		int[] data = {0, 1, 2, 3, 4, 5, 6};
		for (int index : data) {
			new Thread(new MrThread(data[index])).start();
		}

		try {
			// partially return
			preturn("0 arrives without awaiting -1.");
			Thread.sleep(1000000); // simulates calculation or straggler
			preturn("-1 arrives with defering 0.");
			// finally return
			freturn();
			
		} catch (InterruptedException e) {
			// when not all preturns finish
			// and consequently no freturn is invoked.
			e.printStackTrace();
		}
	}
	
	/** Thread that constructs partial return(s) */
	private class MrThread implements Runnable {

		public MrThread(int item) {
			this.item = item;
		}
		
		@Override
		public void run() {
			// partially return
			preturn(item);
		}
		
		private int item;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SampleServer server = new SampleServer("localhost", 3122);
		server.start();
		
	}

}
