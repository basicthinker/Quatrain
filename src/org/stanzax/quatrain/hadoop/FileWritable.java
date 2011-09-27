/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;

import org.stanzax.quatrain.io.ChannelWritable;

/**
 * @author Jinglei Ren
 * This class implements a plain binary transfer protocol for files.
 */
public class FileWritable implements ChannelWritable {
    
    public FileWritable() {
        this(null, 64 * 1024);
    }
    
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
        File path = new File(DefaultPath);
        if (!path.isDirectory()) path.mkdir();
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.ChannelWritable#write(java.nio.channels.SocketChannel)
     */
    @Override
    public long write(SocketChannel channel) throws IOException {
        if (file == null) return 0;

        // write length of file data
        ByteBuffer replyBuffer = ByteBuffer.allocate(8);
        replyBuffer.putLong(0, file.length());
        channel.write(replyBuffer);
        while (replyBuffer.hasRemaining()) {
            channel.write(replyBuffer);
        }
        // write file data
        DataInputStream istream = new DataInputStream(new FileInputStream(file));
        byte[] buf = new byte[BUF_LEN];
        long bytesWritten = 0;
        int n = istream.read(buf);
        while (n != -1) {
            replyBuffer = ByteBuffer.wrap(buf, 0, n);
            bytesWritten += channel.write(replyBuffer);
            while (replyBuffer.hasRemaining()) {
                Thread.yield();
                bytesWritten += channel.write(replyBuffer);
            }
            n = istream.read(buf);
        }
        istream.close();
        if (bytesWritten != file.length()) {
            System.err.println("@FileWritable.write: Incomplete file written.");
        }
        return bytesWritten;
    }

    /* (non-Javadoc)
     * @see org.stanzax.quatrain.io.ChannelWritable#read(java.nio.channels.SocketChannel)
     */
    @Override
    public long read(SocketChannel channel) throws IOException {
        file = new File(DefaultPath + File.separator + 
                Math.abs(new Random().nextInt()) + "@" + System.currentTimeMillis());
        DataOutputStream ostream = new DataOutputStream(
                new FileOutputStream(file));
        
        ByteBuffer lenBuf = ByteBuffer.allocate(8);
        channel.read(lenBuf);
        while (lenBuf.hasRemaining()) channel.read(lenBuf);
        lenBuf.flip();
        long length = lenBuf.getLong();
        
        ByteBuffer buf = ByteBuffer.allocate(length < BUF_LEN ? (int)length : BUF_LEN);
        int bytesRead = 0;
        while (bytesRead < length) {
            if (length - bytesRead < buf.capacity()) {
                buf = ByteBuffer.allocate((int) (length - bytesRead));
            }
            buf.clear();
            bytesRead += channel.read(buf);
            while (buf.hasRemaining()) {
                Thread.yield();
                bytesRead += channel.read(buf); 
            }
            ostream.write(buf.array());
        }
        ostream.close();
        return bytesRead;
    }

    @Override
    public void setValue(Object value) {
        file = (File)value;
    }

    @Override
    public Object getValue() {
        return file;
    }
    
    public String getDefaultPath() {
        return DefaultPath;
    }
    
    public void setDefaultPath(String path) {
        DefaultPath = path;
    }
    
    private File file;
    private static String DefaultPath = "log";

    public final int BUF_LEN;
}
