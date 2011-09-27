/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.File;
import java.io.IOException;

import org.stanzax.quatrain.hadoop.FileWritable;
import org.stanzax.quatrain.hadoop.HadoopWrapper;
import org.stanzax.quatrain.io.Log;
import org.stanzax.quatrain.io.WritableWrapper;
import org.stanzax.quatrain.server.MrServer;

/**
 * @author Jinglei Ren
 *
 */
public class FileServer extends MrServer {

    public FileServer(String address, int port, WritableWrapper wrapper,
            int handlerCount) throws IOException {
        super(address, port, wrapper, handlerCount);
    }
    
    public void FetchDirFiles(String dirPath) {
        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        for (File file : files) {
            preturn(new FileWritable(file));
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
            FileServer server = new FileServer(args[0], Integer.valueOf(args[1]), 
                    new HadoopWrapper(), Integer.valueOf(args[2]));
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
