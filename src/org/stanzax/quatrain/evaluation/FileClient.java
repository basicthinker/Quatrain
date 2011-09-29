/**
 * 
 */
package org.stanzax.quatrain.evaluation;

import java.io.File;
import java.net.InetAddress;

import org.apache.hadoop.io.Text;
import org.stanzax.quatrain.client.MrClient;
import org.stanzax.quatrain.client.ReplySet;
import org.stanzax.quatrain.io.FileWritable;
import org.stanzax.quatrain.io.Log;

/**
 * @author stone
 *
 */
public class FileClient {

    /**
     * @param args - 
     *  args[0] File server address
     *  args[1] Remote port number
     *  args[2] Timeout
     *  args[3] Request path
     *  args[4] Debug log option
     */
    public static void main(String[] args) {
        Log.setDebug(Integer.valueOf(args[4]));
        try {
            MrClient client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), Integer.valueOf(args[2]), null);
            
            File dir = new File("log");
            if (!dir.isDirectory()) dir.mkdir();
            FileWritable writable = new FileWritable(dir);
            ReplySet rs = client.invoke(writable, "FetchDirFiles", new Text(args[3]));
            while (rs.hasMore()) {
                File file = (File)rs.nextElement();
                System.out.println("Fetched " + file.getName());
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
