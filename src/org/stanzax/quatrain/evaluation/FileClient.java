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
            final String target = args[3];
            final MrClient client = new MrClient(InetAddress.getByName(args[0]),
                    Integer.valueOf(args[1]), Integer.valueOf(args[2]), null);
            
            final File dir = new File("log");
            if (!dir.isDirectory()) dir.mkdir();
            for (int i = 0; i < 5; ++i) {
                new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            ReplySet rs = client.invoke(new FileWritable(dir), "FetchDirFiles", 
                                    new Text(target));
                            while (rs.hasMore()) {
                                File file = (File)rs.nextElement();
                                System.out.println("Fetched " + file.getName());
                            }
                            rs.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    
                }).start();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
