package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.ArrayList;

/**
 * Calculate md5 of the string
 */
public final class ArrayStringHashCodeUDF extends UDF {

    public List<Long> evaluate(final List<String> arr) {
        if (arr == null) {
            return null;
        }
        ArrayList<Long> list = new ArrayList<Long>();
        for(String s : arr){
            long h = 1125899906842597L; // prime
            int len = s.length();
            for (int i = 0; i < len; i++) {
                h = 31*h + s.charAt(i);
            }
            list.add(h);
        }
        return list;
    }
}