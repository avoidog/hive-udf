package com.hive.udf.special;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import java.util.HashMap;
import java.util.Map;

@Description(name = "select_min", value = "_FUNC_(expr) - Returns the minimum value of expr corresponding col")
public class SelectMinUDAF extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(SelectMinUDAF.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly Two argument is expected.");
    }
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
    if (!ObjectInspectorUtils.compareSupported(oi)) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Cannot support comparison of map<> type or complex type containing map<>.");
    }
    return new SelectMinEvaluator();
  }

  public static class SelectMinEvaluator extends GenericUDAFEvaluator {

    private transient ObjectInspector inputOI;
    private transient ObjectInspector outputOI;
    private StandardMapObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      

      // init output object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
            inputOI = parameters[0];
            outputOI = parameters[1];
            internalMergeOI = ObjectInspectorFactory.getStandardMapObjectInspector(inputOI, ObjectInspectorUtils.getStandardObjectInspector(outputOI,ObjectInspectorCopyOption.JAVA));
            return internalMergeOI;
      } else {
        // The output of FINAL and COMPLETE is a full aggregation, which is a
        if (!(parameters[0] instanceof StandardMapObjectInspector)) {
            inputOI = parameters[0];
            outputOI = parameters[1];
            internalMergeOI = ObjectInspectorFactory.getStandardMapObjectInspector(inputOI, ObjectInspectorUtils.getStandardObjectInspector(outputOI,ObjectInspectorCopyOption.JAVA));
            return internalMergeOI;
        } else {
            internalMergeOI = (StandardMapObjectInspector) parameters[0];
            inputOI  = internalMergeOI.getMapKeyObjectInspector();
            outputOI = internalMergeOI.getMapValueObjectInspector();
            return ObjectInspectorUtils.getStandardObjectInspector(outputOI,ObjectInspectorCopyOption.JAVA);
        }
      }
    }

    /** class for storing the current min value */
    @AggregationType(estimable = true)
    static class MinAgg extends AbstractAggregationBuffer {
      Object o;
      Object v;
      @Override
      public int estimate() {
        return 32;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MinAgg result = new MinAgg();
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      myagg.o = null;
    }


    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
        assert (parameters.length == 2);
        MinAgg myagg = (MinAgg) agg;
        compare(myagg, parameters[0], parameters[1]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      HashMap<Object, Object> map = new HashMap<Object, Object>();
      map.put(myagg.o, myagg.v);
      return map;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        MinAgg myagg = (MinAgg) agg;
        HashMap<Object, Object> partialResult = (HashMap<Object, Object>) internalMergeOI.getMap(partial);

        for (Map.Entry<Object, Object> entry : partialResult.entrySet()) {
            compare(myagg, entry.getKey(), entry.getValue());
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      return myagg.v;
    }

    private void compare(MinAgg myagg, Object key, Object val){
        int r = ObjectInspectorUtils.compare(myagg.o, inputOI, key, inputOI);
        if (myagg.o == null || r > 0) {
            myagg.o = ObjectInspectorUtils.copyToStandardObject(key, inputOI);
            myagg.v = ObjectInspectorUtils.copyToStandardObject(val, outputOI, ObjectInspectorCopyOption.JAVA);
        }
    }

  }
}