package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Calculate md5 of the string
 */
public final class StringContainsUDF extends UDF {

    public Boolean evaluate(final String s, List<String> arr) {
        if (s == null || arr == null) {
            return null;
        }
        for(String a : arr) {
            if(s.contains(a)){
                return true;
            }
        }
        return false;
    }

    public Boolean evaluate(final String s, String a) {
        if (s == null || a == null) {
            return null;
        }
        if(s.contains(a)){
            return true;
        }
        return false;
    }
}