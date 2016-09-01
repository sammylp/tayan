package org.sam.lock;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * User: sammy
 * Date: 2015/1/7
 * Time: 14:45
 */
public class ErrorStackTraceHelper {

    public static String getStackTraceAsString(Throwable e){

        StringWriter stringWriter = new StringWriter();

        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        StringBuffer error = stringWriter.getBuffer();
        return error.toString();
    }

    public static String getStackTraceAsStringWithParam(Throwable e, String... param){

        String stackErr = getStackTraceAsString(e);
        if(param != null && param.length > 0) {
            for(String str : param) {
                stackErr = stackErr.concat(str);
            }
        }

        return stackErr;
    }
}
