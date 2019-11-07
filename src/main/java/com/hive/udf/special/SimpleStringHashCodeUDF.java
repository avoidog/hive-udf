package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * Calculate md5 of the string
 */
public final class SimpleStringHashCodeUDF extends UDF {

    public Integer evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        return s.toString().hashCode();
    }
}