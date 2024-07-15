/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.impl;

import com.github.vasnake.hive.java.udaf.base.GenericPrimitiveAverageUDAFEvaluator;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;

@SuppressWarnings("deprecation")
public class AverageDoubleUDAFEvaluator extends GenericPrimitiveAverageUDAFEvaluator {

  protected static void debug(String msg) { GenericPrimitiveAverageUDAFEvaluator.debug("AverageDoubleUDAFEvaluator." + msg); }

  protected static class DoubleAverageBuffer implements AggregationBuffer {
    protected long count = 0;
    protected Double sum = 0d;

    public boolean isNull() {
      return (count == 0 && !sum.isNaN());
    }

    @Override public String toString() {
      return "DoubleAverageBuffer{" + "count=" + count + ", sum=" + sum + '}';
    }
  }

  protected AggregationBuffer newAverageAggregationBuffer() {
    return new DoubleAverageBuffer();
  }

  protected void doReset(AggregationBuffer aggregation) {
    // debug("doReset, accum " + aggregation.toString());

    DoubleAverageBuffer accum = (DoubleAverageBuffer) aggregation;
    accum.count = 0;
    accum.sum = 0d;
  }

  protected ObjectInspector getInputFieldJavaObjectInspector() { return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; }
  protected ObjectInspector getSumFieldWritableObjectInspector() { return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector; }

  protected void doTerminatePartial(AggregationBuffer aggregation) {
    // debug("doTerminatePartial enter, accum " + aggregation.toString());

    // TODO: get rid of Writable wrappers
    DoubleAverageBuffer accum = (DoubleAverageBuffer) aggregation;
    ((LongWritable) partialResult[0]).set(accum.count);

    if(partialResult[1] == null) {
      partialResult[1] = new DoubleWritable(accum.sum);
    } else {
      ((DoubleWritable) partialResult[1]).set(accum.sum);
    }

    // debug("doTerminatePartial exit, partialResult " + Arrays.toString(partialResult));
  }

  protected Object doTerminate(AggregationBuffer aggregation) {
    // debug("doTerminate, accum " + aggregation.toString());

    DoubleAverageBuffer accum = (DoubleAverageBuffer) aggregation;
    if(accum.count == 0) {
      if (accum.isNull()) {
        // debug("doTerminate, result is null");
        return null;
      } else {
        // debug("doTerminate, result is nan");
        return new DoubleWritable(Double.NaN);
      }
    } else {
      DoubleWritable result = new DoubleWritable(accum.sum / accum.count);
      // debug("doTerminate, return sum/count " + result);
      return result;
    }
  }

  protected void doIterate(AggregationBuffer aggregation, ObjectInspector oi, Object parameter) {
    // expected not null values
    // nan processing, should ignore unless previous input is null
    // debug("doIterate enter, accum " + aggregation.toString() + "; OI " + oi + ", parameter " + parameter);

    DoubleAverageBuffer accum = (DoubleAverageBuffer) aggregation;
    double value = PrimitiveObjectInspectorUtils.getDouble(parameter, (PrimitiveObjectInspector) oi);


    if (Double.isNaN(value)) { // if accum is null: set accum to nan, ignore otherwise
      if (accum.isNull()) {
        // debug("doIterate, input is nan and sum is null, set sum to nan");
        accum.sum = value;
      } // else { debug("doIterate, input is nan and sum is not null, ignore"); }
    } else { // input is not nan
      if (accum.count == 0) accum.sum = value; // sum was nan or 0
      else accum.sum += value;
      accum.count++;
    }

    // debug("doIterate exit, accum " + aggregation.toString());
  }

  protected void doMerge(AggregationBuffer aggregation, Long partialCount, ObjectInspector sumFieldOI, Object partialSum) {
    // debug("doMerge enter, accum " + aggregation.toString() + "; partialCount " + partialCount + "; sumOI " + sumFieldOI + ", partialSumValue " + partialSum);

    DoubleAverageBuffer accum = (DoubleAverageBuffer) aggregation;
    double ps = ((DoubleObjectInspector)sumFieldOI).get(partialSum); // could be nan

    if (partialCount == 0) { // ps null or nan
      if (Double.isNaN(ps)) { // ps is nan, ignore if accum is not null
        if (accum.count == 0) { // accum is null or nan, ps is nan
          accum.sum = ps;
        } // else: accum is good, ps is nan
      } // else: ps is null, ignore
    } else { // partial is good
      if (accum.sum.isNaN()) accum.sum = ps;
      else accum.sum += ps;
      accum.count += partialCount;
    }

    // debug("doMerge exit, accum " + aggregation.toString());
  }

  // TODO: DRY, add type parameter, abstract methods and rewrite AverageDouble, AverageFloat (see most_freq for example)

}
