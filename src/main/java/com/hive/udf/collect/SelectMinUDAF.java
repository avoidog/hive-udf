package com.hive.udf.collect;

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
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

@Description(name = "select_min", value = "_FUNC_(expr) - Returns the minimum value of expr corresponding col")
public class SelectMinUDAF extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(SelectMinUDAF.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length < 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "as least Two argument is expected.");
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
    private transient ObjectInspector[] outputOIs;
    private transient int size;
    private transient StandardStructObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      // init output object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
            inputOI = parameters[0];
            size = parameters.length;
            outputOIs = new ObjectInspector[size];
            for(int i = 0; i < size; i++){
              outputOIs[i] = parameters[i];
            }
            internalMergeOI = makeStructOI();
            return internalMergeOI;
      } else {
        // The output of FINAL and COMPLETE is a full aggregation, which is a
        if (!(parameters[0] instanceof StandardStructObjectInspector)) {
            inputOI = parameters[0];
            size = parameters.length;
            outputOIs = new ObjectInspector[size];
            for(int i = 0; i < size; i++){
              outputOIs[i] = parameters[i];
            }
            internalMergeOI = makeStructOI();
            return internalMergeOI;
        } else {
            internalMergeOI = (StandardStructObjectInspector) parameters[0];
            List<StructField> oilist = (List<StructField>)internalMergeOI.getAllStructFieldRefs();
            
            inputOI = oilist.get(0).getFieldObjectInspector();
            size = oilist.size();
            outputOIs = new ObjectInspector[size];
            for(int i = 0; i < size; i++){
              outputOIs[i] = oilist.get(i).getFieldObjectInspector();
            }
            return makeStructOI();
        }
      }
    }

    private StandardStructObjectInspector makeStructOI() {
      ArrayList<String> fieldName = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOI = new ArrayList<ObjectInspector>(); 

      fieldName.add("f0");
      fieldOI.add(inputOI);
      for(int i = 1; i < size; i++){
        fieldName.add("f" + i);
        fieldOI.add(ObjectInspectorUtils.getStandardObjectInspector(outputOIs[i], ObjectInspectorCopyOption.JAVA));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOI);
    }

    /** class for storing the current min value */
    @AggregationType(estimable = true)
    static class MinAgg extends AbstractAggregationBuffer {
      Object o;
      Object[] v;
      @Override
      public int estimate() {
        if(v != null) {
          return 32 + 16 * v.length;
        } else {
          return 32;
        }
      }
      public void refresh(int s){
        v = new Object[s];
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MinAgg result = new MinAgg();
      result.refresh(size);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      myagg.o = null;
      myagg.v = new Object[size];
    }


    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
        MinAgg myagg = (MinAgg) agg;
        compare(myagg, parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      return myagg.v;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        MinAgg myagg = (MinAgg) agg;
        List<Object> partialResult = (List<Object>) internalMergeOI.getStructFieldsDataAsList(partial);
        compare(myagg, partialResult.toArray());
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      return myagg.v;
    }

    private void compare(MinAgg myagg, Object[] args){
        int r = ObjectInspectorUtils.compare(myagg.o, inputOI, args[0], inputOI);
        if (myagg.o == null || r > 0) {
            myagg.refresh(size);
            myagg.o = ObjectInspectorUtils.copyToStandardObject(args[0], inputOI);
            myagg.v[0] = myagg.o;
            for(int i = 1; i < size; i++){
              myagg.v[i] = ObjectInspectorUtils.copyToStandardObject(args[i], outputOIs[i], ObjectInspectorCopyOption.JAVA);
            }
        }
    }
  }
}