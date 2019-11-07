package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Calculate md5 of the string
 */
public final class SimpleStringContainsUDF extends UDF {

    public String evaluate(final String s, List<String> arr) {
        if (s == null || arr == null) {
            return null;
        }
        for(String a : arr) {
            if(s.contains(a)){
                return a;
            }
        }
        return null;
    }

    public String evaluate(final String s, String a) {
        if (s == null || a == null) {
            return null;
        }
        if(s.contains(a)){
            return a;
        }
        return null;
    }
}