/**
 * 
 */
package org.stanzax.quatrain.io;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author basicthinker
 * 
 */
public class Log {
    
    public static boolean DEBUG = false;
    public static PrintStream out = System.out;
    
    public static void setDebug(int option) {
        if ((option & NONE) == 0) {
            DEBUG = false;
            return;
        } else DEBUG = true;
        
        if ((option & ACTION) == 1)
            action = true;
        if ((option & STATE) == 1)
            state = true;
    }
    
    public static void state(int frequency, String info, Object... values) {
        if (!state) return;
        if ((int)(Math.random() * frequency) == 0)
            log(" -- -- -> STATE @", MS_TIME, info, values);
    }
    
    public static void action(String info, Object...values) {
        if (!action) return;
        log(" -- -> DEBUG @", MS_TIME, info, values);
    }

    public static void info(String info, Object...values) {
        log(" -> INFO @", DATE_TIME, info, values);
    }
    
    private static void log(String header, int timeFormat, String info, Object[] values) {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append(header);
        if (timeFormat == DATE_TIME) strBuf.append(currentTime());
        else if (timeFormat == MS_TIME) strBuf.append(System.currentTimeMillis());
        
        strBuf.append(" - ").append(info);
        for (Object value : values) {
            strBuf.append(" : ").append(value);
        }
        Log.out.println(strBuf.toString());
    } 
    
    private static String currentTime() {
        SimpleDateFormat formatter = 
            new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        return formatter.format(new Date());
    }
    
    public static final int NONE = 0xf;
    public static final int ACTION = 0x1;
    public static final int STATE = 0x2;
    
    public static final int DATE_TIME = 1;
    public static final int MS_TIME = 2;
    
    private static boolean action = false;
    private static boolean state = false;
}
