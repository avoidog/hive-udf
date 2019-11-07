package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;


/**
 * Calculate md5 of the string
 */
public final class StringToBytesUDF extends UDF {

    public byte[] evaluate(final String s) {
        if (s == null) {
            return null;
        }
        return s.getBytes();
    }
}