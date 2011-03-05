/**
 * 
 */
package org.stanzax.quatrain.io;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author basicthinker
 * 
 */
public class Log {
    
    public static boolean debug = false;
    
    public static void setDebug(int option) {
        if (option == NONE) {
            debug = false;
            return;
        } else debug = true;
        if (option >= STATE) {
            state = true;
            option -= STATE;
        } else state = false;
        if (option >= ACTION) {
            action = true;
            option -= ACTION;
        } else action = false;
    }
    
    public static void state(int frequency, String info, Object... values) {
        if (!debug || !state) return;
        if ((int)(Math.random() * frequency) == 0)
            log(" -- -- -> STATE @", info, values);
    }
    
    public static void action(String info, Object...values) {
        if (!debug || !action) return;
        log(" -- -> DEBUG @", info, values);
    }

    public static void info(String info, Object...values) {
        log(" -> INFO @", info, values);
    }
    
    private static void log(String header, String info, Object[] values) {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append(header).append(currentTime()).append(" - ").append(info);
        for (Object value : values) {
            strBuf.append(" : ").append(value);
        }
        System.out.println(strBuf);
    } 
    
    private static String currentTime() {
        SimpleDateFormat formatter = 
            new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        return formatter.format(new Date());
    }
    
    public static final int NONE = 0;
    public static final int ACTION = 1;
    public static final int STATE = 2;
    
    private static boolean action = false;
    private static boolean state = false;
}
