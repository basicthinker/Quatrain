/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;


/**
 * @author Jinglei Ren
 * This class implements a plain binary transfer protocol for files.
 */
public class FileWritable implements DirectWritable {
    
    public FileWritable(File file) {
        this(file, 64 * 1024);
    }
    
    /**
     * @param filePath - Path of file to write, or path to store files, each of which is retrieved by {@link #getValue() getValue} 
     *                  after respective reading
     * @param bufLen - Length of byte buffer, 64 * 1024 recommended
     * */
    public FileWritable(File file, int bufLen) {
        this.file = file;
        this.BUF_LEN = bufLen;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.DirectWritable#write(java.nio.channels.SocketChannel)
     */
    @Override
    public long write(OutputStream ostream) throws IOException {
        if (file == null) return 0;
        long length = file.length();
        
        // write length of file data
        DataOutputStream dstream = new DataOutputStream(ostream);
        dstream.writeLong(length);

        // write file data
        DataInputStream fstream = new DataInputStream(new FileInputStream(file));
        byte[] buf = new byte[length < BUF_LEN ? (int)length : BUF_LEN];
        long bytesWritten = 0;
        int n = fstream.read(buf);
        while (n != -1) { 
            ostream.write(buf, 0, n);
            bytesWritten += n;
            n = fstream.read(buf);
        }
        fstream.close();
        
        if (bytesWritten != length) {
            System.err.println("@FileWritable.write: Incomplete file written.");
        }
        return bytesWritten;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.DirectWritable#read(java.nio.channels.SocketChannel)
     */
    @Override
    public long read(InputStream istream) throws IOException {
        if (file.isDirectory()) {
            file = new File(file.getPath() + File.separator + 
                    Math.abs(new Random().nextInt()) + "@" + System.currentTimeMillis());
        } else {
            file = new File(file.getParent() + File.separator + 
                    Math.abs(new Random().nextInt()) + "@" + System.currentTimeMillis());
        }
        DataOutputStream ostream = new DataOutputStream(
                new FileOutputStream(file));
        
        DataInputStream dstream = new DataInputStream(istream);
        final long length = dstream.readLong();
        
        byte[] buf = new byte[length < BUF_LEN ? (int)length : BUF_LEN];
        long balance = length;
        int n = 0;
        while (balance > 0) {
            n = istream.read(buf, 0, balance < BUF_LEN ? (int)balance : BUF_LEN);
            balance -= n;
            ostream.write(buf, 0, n);
        }
        ostream.close();
        return length;
    }

    @Override
    public void setValue(Object value) {
        file = (File)value;
    }

    @Override
    public Object getValue() {
        return file;
    }
    
    private File file;
    public final int BUF_LEN;
}
