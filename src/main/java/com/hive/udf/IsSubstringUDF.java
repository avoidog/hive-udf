package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Calculate md5 of the string
 */
public final class IsSubstringUDF extends UDF {

    public Boolean evaluate(final Text s, final Text t) {
        if (s == null || t == null || s.getLength() <= 0 || t.getLength() <= 0) {
            return false;
        }
        return s.toString().contains(t.toString());
    }
}