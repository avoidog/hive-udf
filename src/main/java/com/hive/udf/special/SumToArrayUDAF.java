package com.hive.udf.special;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

@Description(name = "array_dim_sum",
        value = "_FUNC_(arr, val) - Returns an map of val sum to exactly arr value, arr element as key, sum(val) as value"
)
public class SumToArrayUDAF extends AbstractGenericUDAFResolver {
    public static final Logger LOG = Logger.getLogger(SumToArrayUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                "Exactly two argument is expected.");
        }

        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only primitive type arguments are accepted but "
                    + parameters[0].getTypeName() + " is passed.");
        }

        switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
              return new SumLongUDAFEvaluator();
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case VARCHAR:
            case CHAR:
              return new SumDoubleUDAFEvaluator();
            case DECIMAL:
              //return new GenericUDAFSumHiveDecimal();
            case BOOLEAN:
            case DATE:
            default:
              throw new UDFArgumentTypeException(0,
                  "Only numeric or string type arguments are accepted but "
                      + parameters[0].getTypeName() + " is passed.");
        }
    }

    public static class SumLongUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private ListObjectInspector inputKeyListOI;
        private PrimitiveObjectInspector inputKeyOI;
        private PrimitiveObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector internalMergeOI;

        // Aggregation buffer definition and manipulation methods
        @AggregationType(estimable = true)
        static class MapAggBuffer extends AbstractAggregationBuffer {
            private HashMap<Object, Long> dataMap;// = new HashMap<Object, Long>();

            @Override
            public int estimate() {
                return dataMap.size() * 64;
            }

            public void reset() {
                dataMap = new HashMap<Object, Long>();
            }
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            LOG.error(" ArrayDimSum.init() - Mode= " + m.name());
            for (int i = 0; i < parameters.length; ++i) {
                LOG.error(" ObjectInspector[ " + i + " ] = " + parameters[0]);
            }

            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1 || m == m.COMPLETE) {
                inputKeyListOI = (ListObjectInspector) parameters[0];
                inputValOI = (PrimitiveObjectInspector) parameters[1];
                inputKeyOI = (PrimitiveObjectInspector) inputKeyListOI.getListElementObjectInspector();
            } else {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];
                inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                inputKeyListOI = ObjectInspectorFactory.getStandardListObjectInspector(inputKeyOI);
            }

            return ObjectInspectorFactory.getStandardMapObjectInspector(
                inputKeyOI,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAggBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object k = parameters[0];
            Object v = parameters[1];
            if(k == null || v == null) {
                return;
            }

            MapAggBuffer myagg = (MapAggBuffer) agg;
            Long val = PrimitiveObjectInspectorUtils.getLong(v, inputValOI);

            if (val != 0) {
                for(Object key : inputKeyListOI.getList(k)){
                    Object o = ObjectInspectorUtils.copyToStandardObject(key, inputKeyOI);
                    myagg.dataMap.put(o, myagg.dataMap.getOrDefault(o, 0L) + val);
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);

            for(Object key : partialResult.keySet()){
                Long val = ((LongWritable)partialResult.get(key)).get();
                Object k = inputKeyOI.copyObject(key);
                myagg.dataMap.put(k, myagg.dataMap.getOrDefault(k, 0L) + val);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            return new HashMap<Object, Object>(myagg.dataMap);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            return new HashMap<Object, Object>(myagg.dataMap);
        }
    }




    public static class SumDoubleUDAFEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private ListObjectInspector inputKeyListOI;
        private PrimitiveObjectInspector inputKeyOI;
        private PrimitiveObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardMapObjectInspector internalMergeOI;


        // Aggregation buffer definition and manipulation methods
        @AggregationType(estimable = true)
        static class MapAggBuffer extends AbstractAggregationBuffer {
            HashMap<Object, Double> dataMap; //= new HashMap<Object, Double>();

            @Override
            public int estimate() {
                return dataMap.size() * 64;
            }

            public void reset() {
                dataMap = new HashMap<Object, Double>();
            }
        }

        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            LOG.error(" ArrayDimSum.init() - Mode= " + m.name());
            for (int i = 0; i < parameters.length; ++i) {
                LOG.error(" ObjectInspector[ " + i + " ] = " + parameters[0]);
            }

            // init output object inspectors
            // The output of a partial aggregation is a map
            if (m == Mode.PARTIAL1 || m == m.COMPLETE) {
                inputKeyListOI = (ListObjectInspector) parameters[0];
                inputValOI = (PrimitiveObjectInspector) parameters[1];
                inputKeyOI = (PrimitiveObjectInspector) inputKeyListOI.getListElementObjectInspector();
            } else {
                if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                    inputKeyListOI = (StandardListObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector)inputKeyListOI.getListElementObjectInspector();
                    inputValOI = (PrimitiveObjectInspector) parameters[1];
                } else {
                    internalMergeOI = (StandardMapObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                    inputValOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                    inputKeyListOI = ObjectInspectorFactory.getStandardListObjectInspector(inputKeyOI);
                }
            }
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    inputKeyOI,
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAggBuffer buff = new MapAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            Object k = parameters[0];
            Object v = parameters[1];
            if (k == null || v == null) {
                return;
            }

            MapAggBuffer myagg = (MapAggBuffer) agg;
            Double val = PrimitiveObjectInspectorUtils.getDouble(v, inputValOI);

            if (val != 0) {
                for(Object key : inputKeyListOI.getList(k)){

                    Object o = inputKeyOI.copyObject(key);
                    myagg.dataMap.put(o, myagg.dataMap.getOrDefault(o, 0.0) + val);
                }
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);

            for(Object key : partialResult.keySet()){
                Double val = ((DoubleWritable)partialResult.get(key)).get();;
                Object k = inputKeyOI.copyObject(key);
                myagg.dataMap.put(k, myagg.dataMap.getOrDefault(k, 0.0) + val);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            MapAggBuffer arrayBuff = (MapAggBuffer) buff;
            arrayBuff.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggBuffer myagg = (MapAggBuffer) agg;
            return new HashMap<Object, Object>(myagg.dataMap);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            MapAggBuffer myagg = (MapAggBuffer) agg;
            return new HashMap<Object, Object>(myagg.dataMap);
        }
    }
}