package com.hive.udf.special;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Calculate map_to_str
 */
public final class SimpleMapToStrUDF extends UDF {

    public String evaluate(final Map<String, Long> map, int x) {
        if (map == null) {
            return null;
        }
        PrettyPrintingMap ppm = new PrettyPrintingMap<String, Long>(map); 
        return ppm.toString();
    }

    public String evaluate(final Map<String, Double> map, double x) {
        if (map == null) {
            return null;
        }
        PrettyPrintingDoubleMap ppm = new PrettyPrintingDoubleMap<String>(map); 
        return ppm.toString();
    }
}

class PrettyPrintingMap<K, V> {
    private Map<K, V> map;

    public PrettyPrintingMap(Map<K, V> map) {
        this.map = map;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<K, V> entry = iter.next();
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(entry.getValue());
            if (iter.hasNext()) {
                sb.append(',');
            }
        }
        return sb.toString();
    }
}

class PrettyPrintingDoubleMap<K>  {
    private Map<K, Double> map;

    public PrettyPrintingDoubleMap(Map<K, Double> map) {
        this.map = map;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<K, Double>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<K, Double> entry = iter.next();
            sb.append(entry.getKey());
            sb.append(':');
            sb.append(String.format("%.3f", entry.getValue()));
            if (iter.hasNext()) {
                sb.append(',');
            }
        }
        return sb.toString();
    }
}