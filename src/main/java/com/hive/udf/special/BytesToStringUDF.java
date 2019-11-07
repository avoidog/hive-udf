package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;

/**
 * Calculate md5 of the string
 */
public final class BytesToStringUDF extends UDF {

    public String evaluate(List<Byte> arr) {
        if (arr == null) {
            return null;
        }
        Byte[] buffer = new Byte[arr.size()];  //  new byte[arr.size()];
        arr.toArray(buffer);
        byte[] b =  ArrayUtils.toPrimitive(buffer);
        return new String(b);
    }
}