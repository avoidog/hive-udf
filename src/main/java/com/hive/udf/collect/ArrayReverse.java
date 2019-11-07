package com.hive.udf.collect;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


/**
 * Based on java.util.Arrays.copyOfRange
 */

public class ArrayReverse extends GenericUDF {
    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {

        Object uninspListObj = arg0[0].get();

        int listSize = listInspector.getListLength(uninspListObj);

        List truncatedListObj = (List) returnInspector.create(0);

        for (int i = listSize - 1; i >= 0 ; i--) {
            truncatedListObj.add(listInspector.getListElement(uninspListObj, i));
            
        }
        return truncatedListObj;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "array_denull(" + arg0[0] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        ObjectInspector first = arg0[0];
        if (first.getCategory() == Category.LIST) {
            listInspector = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array as arguments ");
        }


        returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                listInspector.getListElementObjectInspector());
        return returnInspector;
    }

}
