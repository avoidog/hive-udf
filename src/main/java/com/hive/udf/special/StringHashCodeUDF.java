package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * Calculate md5 of the string
 */
public final class StringHashCodeUDF extends UDF {

    public Long evaluate(final String s) {
        if (s == null) {
            return null;
        }
        long h = 1125899906842597L; // prime
        int len = s.length();
        for (int i = 0; i < len; i++) {
            h = 31*h + s.charAt(i);
        }
        return h;
    }
}