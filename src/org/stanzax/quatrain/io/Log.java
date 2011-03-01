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
    
    public static void setDebug(boolean isDebug) {
        debug = isDebug;
    }
    
    public static void debug(String info, Object...values) {
        log(" - DEBUG @", info, values);
    }

    public static void info(String info, Object...values) {
        log("INFO @", info, values);
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
    
}
