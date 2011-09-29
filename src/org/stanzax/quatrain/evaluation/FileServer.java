/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.stanzax.quatrain.io.FileWritable;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author Jinglei Ren
 *
 */
public class FileServer extends MrServer {

    public FileServer(String address, int port, int handlerCount)
    		throws IOException {
        super(address, port, handlerCount, null);
    }
    
    public void FetchDirFiles(Text dirPath) {
        File dir = new File(dirPath.toString());
        File[] files = dir.listFiles();
        for (File file : files) {
            if (!file.isDirectory()) {
            	preturn(new FileWritable(file));
            }
        }
    }
    
    /**
     * @param args - 
     *          args[0] Server address </br>
     *          args[1] Port number </br>
     *          args[2] Number of handlers </br>
     *          args[3] Debug log option
     */
    public static void main(String[] args) {
        Log.setDebug(Integer.valueOf(args[3]));
        try {
            FileServer server = new FileServer(args[0], 
            		Integer.valueOf(args[1]), Integer.valueOf(args[2]));
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
