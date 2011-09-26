/**
 * 
 */
package org.stanzax.quatrain.hadoop;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;

import org.stanzax.quatrain.io.ChannelWritable;

/**
 * @author stone
 *
 */
public class ChannelWritableFactory {

    public static ChannelWritable wrap(File file) {
        return new FileWritable(file.getAbsolutePath(), 64 * 1024);
    }
    
    public static ChannelWritable newInstance(
            Class<? extends ChannelWritable> type) throws Exception {
        Constructor<?> constructor = ConstructorPool.get(type);
        if (constructor == null) {
            constructor = type.getConstructor(new Class[]{});
            constructor.setAccessible(true);
            ConstructorPool.put(type, constructor);
        }
        return (ChannelWritable) constructor.newInstance();
    }
    
    private static ConcurrentHashMap<Type, Constructor<?>> ConstructorPool = 
            new ConcurrentHashMap<Type, Constructor<?>>();
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // For basic test
        File file = new File("log/test.txt");
        ChannelWritable writable = ChannelWritableFactory.wrap(file);
        System.out.println("Created by wrap: " + writable.toString());
        try {
            writable = ChannelWritableFactory.newInstance(FileWritable.class);
            System.out.println("Created by newInstance: " + writable.toString());
            writable = ChannelWritableFactory.newInstance(FileWritable.class);
            System.out.println("Created in cache (#): " + writable.toString() +
                    " (" + ConstructorPool.size() + ")");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
