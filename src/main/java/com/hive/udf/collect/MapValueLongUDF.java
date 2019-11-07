package com.hive.udf.collect;



import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;



/**
 * Return a map minus key value pairs from a map, for a given set of keys.
 *
 * @author otistamp
 */

@Description(name = "map_remove_keys",
        value = "_FUNC_(map, key_array) - Returns the sorted entries of a map minus key value pairs, the for a given set of keys "
)
public class MapValueLongUDF extends GenericUDF {
    private PrimitiveObjectInspector inputKeyOI;
    private MapObjectInspector mapInspector;
    private StandardMapObjectInspector retValInspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Map hiveMap = mapInspector.getMap(arg0[0].get());

        Map retVal = (Map) retValInspector.create();

        for (Object hiveKey : hiveMap.keySet()){
                Object hiveVal = hiveMap.get(hiveKey);
                Object keyStd = ObjectInspectorUtils.copyToStandardObject(hiveKey, mapInspector.getMapKeyObjectInspector());
                Object valStd = ObjectInspectorUtils.copyToStandardObject(hiveVal, mapInspector.getMapValueObjectInspector());
                if (valStd instanceof String)
                    retVal.put(keyStd, Long.parseLong((String)valStd));
                if (valStd instanceof Text) 
                    retVal.put(keyStd, Long.parseLong(((Text)valStd).toString()));
        }
        return retVal;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "mvl(" + arg0[0] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        ObjectInspector first = arg0[0];
        if (first.getCategory() == Category.MAP) {
            mapInspector = (MapObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting a map as first argument ");
        }

        inputKeyOI = (PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector();

        retValInspector = (StandardMapObjectInspector) ObjectInspectorFactory.getStandardMapObjectInspector(
            inputKeyOI,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return retValInspector;
    }

}
