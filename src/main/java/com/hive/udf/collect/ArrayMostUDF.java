package com.hive.udf.collect;



import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.log4j.Logger;


/**
 * return most n frequent element in array
 *
 */

@Description(name = "array_union",
		value = "_FUNC_(array1, array2, ...) - Returns the union of a set of arrays "
)
public class ArrayMostUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(ArrayMostUDF.class);
	private StandardListObjectInspector retValInspector;
	private ListObjectInspector listInspector;
	private IntObjectInspector intInspector;

	private class InspectableObject implements Comparable {
		public Object o;
		public ObjectInspector oi;

		public InspectableObject(Object o, ObjectInspector oi) {
			this.o = o;
			this.oi = oi;
		}

		@Override
		public int hashCode() {
			return ObjectInspectorUtils.hashCode(o, oi);
		}

		@Override
		public int compareTo(Object arg0) {
			InspectableObject otherInsp = (InspectableObject) arg0;
			return ObjectInspectorUtils.compare(o, oi, otherInsp.o, otherInsp.oi);
		}

		@Override
		public boolean equals(Object other) {
			return compareTo(other) == 0;
		}
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		int length = 1;
		if(null != intInspector){
			length = intInspector.get(arg0[1].get());
		}
		if(length < 1){
			length = 1;
		}

		Map<InspectableObject, Integer> objects = new HashMap<>();

		Object undeferred = arg0[0].get();
		for (int j = 0; j < listInspector.getListLength(undeferred); ++j) {
			Object nonStd = listInspector.getListElement(undeferred, j);
			InspectableObject stdInsp = new InspectableObject(nonStd, listInspector.getListElementObjectInspector());
			if (objects.containsKey(stdInsp)) {
				objects.put(stdInsp, objects.get(stdInsp) + 1);
			} else {
				objects.put(stdInsp, 1);
			}
		}
		List<Entry<InspectableObject, Integer>> list = new ArrayList<>(objects.entrySet());
        list.sort(Entry.comparingByValue());
		Collections.reverse(list);
		List retVal = (List) retValInspector.create(0);

		for(int i = 0; i < length && i < list.size(); i++){
			InspectableObject inspObj = (InspectableObject) list.get(i).getKey();
			Object stdObj = ObjectInspectorUtils.copyToStandardObject(inspObj.o, inspObj.oi);
			retVal.add(stdObj);
		}

		return retVal;
	}


	@Override
	public String getDisplayString(String[] arg0) {
		if(arg0.length > 1)
			return "array_most(" + arg0[0] + ", " + arg0[1] + " )";
		else
			return "array_most(" + arg0[0] + " )";
	}


	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if (arg0.length > 2) {
			throw new UDFArgumentException(" Expecting at most two arrays as arguments ");
		}
		ObjectInspector first = arg0[0];
        if (first.getCategory() == Category.LIST) {
            listInspector = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array, one optional int as arguments ");
        }

		if (arg0.length > 1) {
			ObjectInspector second = arg0[1];
			if (second.getCategory() == Category.PRIMITIVE) {
				PrimitiveObjectInspector secondPrim = (PrimitiveObjectInspector) second;
				if (secondPrim.getPrimitiveCategory() == PrimitiveCategory.INT) {
					intInspector = (IntObjectInspector) second;
				} else {
					throw new UDFArgumentException(" Expecting an array, one optional int as arguments ");
				}
			} else {
				throw new UDFArgumentException(" Expecting an array, one optional int as arguments ");
			}
		}
		
		retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
		return retValInspector;
	}

}
