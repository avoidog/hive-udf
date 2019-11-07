package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;


/**
 * format *.00.00.*  -> pad second and thrid version number to at least length 2
 */
public final class VersionFormatUDF extends UDF {

    public String evaluate(final String s, final String format) {
        return evaluate(s, format, "\\.");
    }

    public String evaluate(final String s, final String format, final String sep) {
        if (s == null || format == null) {
            return s;
        }
        String[] ss = s.split(sep);
        String[] fs = format.split(sep);
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < ss.length; i++){
            if(i < fs.length){
                if(fs[i].equals("*")){
                    builder.append(ss[i]);
                } else {
                    int diff = fs[i].length() - ss[i].length();
                    while(diff > 0){
                        builder.append("0");
                        diff-=1;
                    }
                    builder.append(ss[i]);
                }
            } else {
                builder.append(ss[i]);
            }
            builder.append(".");
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }
}