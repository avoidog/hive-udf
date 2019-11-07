package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.ArrayList;

/**
 * Calculate md5 of the string
 */
public final class SimpleArrayStringHashCodeUDF extends UDF {

    public List<Integer> evaluate(final List<String> arr) {
        if (arr == null) {
            return null;
        }
        ArrayList<Integer> list = new ArrayList<Integer>();
        for(String s : arr){
            list.add(s.hashCode());
        }
        return list;
    }
}