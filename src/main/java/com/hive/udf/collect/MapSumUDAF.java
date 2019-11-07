package com.hive.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.HashMap;
import java.util.Map;


@Description(name = "map_sum",
        value = "_FUNC_(x) - Returns a map which contains the union of an aggregation of maps "
)
public class MapSumUDAF extends AbstractGenericUDAFResolver {


    /// Snarfed from Hives CollectSet UDAF

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        // TODO Auto-generated method stub
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "One argument is expected; map<string, bigint>");
        }
        TypeInfo paramType = parameters[0];
        if (paramType.getCategory() == Category.MAP) {
            return new MapUnionUDAFEvaluator();
        } else {
            //// Only maps for now
            throw new UDFArgumentTypeException(0, " Only maps supported for now ");
        }
    }


    public static class MapUnionUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private MapObjectInspector inputMapOI;
        private ObjectInspector inputKeyOI;
        private ObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector moi;
        private StandardMapObjectInspector internalMergeOI;


        static class MapAggBuffer implements AggregationBuffer {
            HashMap<Object, Object> collectMap = new HashMap<Object, Object>();
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputMapOI = (MapObjectInspector) parameters[0];

                inputKeyOI = inputMapOI.getMapKeyObjectInspector();
                inputValOI = inputMapOI.getMapValueObjectInspector();

                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
                        ObjectInspectorUtils.getStandardObjectInspector(inputValOI));
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    inputKeyOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    inputValOI = ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return (StandardMapObjectInspector) ObjectInspectorFactory
                            .getStandardMapObjectInspector(inputKeyOI, inputValOI);
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = internalMergeOI.getMapValueObjectInspector();
                    moi = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return moi;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if (parameters == null) {
                return;
            }

            Object mpObj = parameters[0];


            if (mpObj != null) {
                MapAggBuffer myagg = (MapAggBuffer) agg;
                Map mp = inputMapOI.getMap(mpObj);
                for (Object k : mp.keySet()) {
                    Object v = mp.get(k);
                    putIntoSet(k, v, myagg);
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
            for (Object i : partialResult.keySet()) {
                putIntoSet(i, partialResult.get(i), myagg);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.collectMap = new HashMap<Object, Object>();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;

        }

        private void putIntoSet(Object key, Object val, MapAggBuffer myagg) {
            Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI);
            Object valCopy = ObjectInspectorUtils.copyToStandardObject(val, this.inputValOI);

            if(myagg.collectMap.containsKey(keyCopy)){
                Object acc = myagg.collectMap.get(keyCopy);
                if(acc instanceof IntWritable && valCopy instanceof IntWritable){
                    int int_val = ((IntWritable) acc).get() + ((IntWritable) valCopy).get();
                    myagg.collectMap.put(keyCopy, new IntWritable(int_val));

                } else if(acc instanceof LongWritable && valCopy instanceof LongWritable){
                    long long_val = ((LongWritable) acc).get() + ((LongWritable) valCopy).get();
                    myagg.collectMap.put(keyCopy, new LongWritable(long_val));

                } else if(acc instanceof DoubleWritable && valCopy instanceof DoubleWritable){
                    double double_val = ((DoubleWritable) acc).get() + ((DoubleWritable) valCopy).get();
                    myagg.collectMap.put(keyCopy, new DoubleWritable(double_val));

                } else if(acc instanceof Integer && valCopy instanceof Integer){
                    int int_val = ((Integer) acc) + ((Integer) valCopy);
                    myagg.collectMap.put(keyCopy, int_val);

                }  else if(acc instanceof Long && valCopy instanceof Long){
                    long long_val = ((Long) acc) + ((Long) valCopy);
                    myagg.collectMap.put(keyCopy, long_val);

                }  else if(acc instanceof Double && valCopy instanceof Double){
                    double double_val = ((Double) acc) + ((Double) valCopy);
                    myagg.collectMap.put(keyCopy, double_val);
                }
            } else {
                myagg.collectMap.put(keyCopy, valCopy);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            HashMap<Object, Object> ret = new HashMap<Object, Object>(myagg.collectMap);
            return ret;
        }
    }


}