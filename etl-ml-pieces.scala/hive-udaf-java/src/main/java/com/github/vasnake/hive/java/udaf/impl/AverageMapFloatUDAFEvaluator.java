/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.impl;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.github.vasnake.hive.java.udaf.base.GenericMapAverageUDAFEvaluator;

@SuppressWarnings("deprecation")
public class AverageMapFloatUDAFEvaluator extends GenericMapAverageUDAFEvaluator {

    protected static void debug(String msg) { GenericMapAverageUDAFEvaluator.debug("AverageMapFloatUDAFEvaluator." + msg); }

    protected static class MapFloatAverageBuffer implements AggregationBuffer {
        // rules:
        // count=0 and sum=0: sum is null,
        // count=0 and sum.isNaN: sum is nan;
        // ignore null, ignore nan if sum is not null;
        // null collection: result is null,
        // null collection and empty collection: result is empty collection.
        protected Map<String, Long> counts = null; // 0..max
        protected Map<String, Float> sums = null; // 0, nan, any float, no null!

        protected void newEmptyBuffer() {
            counts = new HashMap<>();
            sums = new HashMap<>();
        }

        @Override public String toString() {
            return "MapFloatAverageBuffer{" + "counts=" + counts + ", sums=" + sums + '}';
        }
    }

    protected static class FloatMapAverage extends GenericMapAverageUDAFEvaluator.NumericMapAverage<Float> {
        @Override public boolean isNaN(Float x) { return x.isNaN(); }
        @Override public Float sum(Float a, Float b) { return a + b; }
        @Override public Float div(Float a, Long b) { return a / b; }
        @Override public Float zero() { return 0f; }
    }

    private final static FloatMapAverage mapProcessor = new FloatMapAverage();

    @Override protected AggregationBuffer newAverageAggregationBuffer() {
        return new MapFloatAverageBuffer();
    }

    @Override
    protected void doReset(AggregationBuffer aggregation) {
        // debug("doReset, accum " + aggregation.toString());

        MapFloatAverageBuffer accum = (MapFloatAverageBuffer) aggregation;
        if (accum.counts != null) accum.counts.clear();
        if (accum.sums != null) accum.sums.clear();

        accum.counts = null;
        accum.sums = null;
    }

    @Override
    protected ObjectInspector getInputFieldJavaObjectInspector() {
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaFloatObjectInspector
        );
    }

    @Override
    protected ObjectInspector getSumFieldWritableObjectInspector() {
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaFloatObjectInspector
        );
    }

    @Override
    protected void doTerminatePartial(AggregationBuffer aggregation) {
        // debug("doTerminatePartial enter, accum " + aggregation.toString());

        MapFloatAverageBuffer accum = (MapFloatAverageBuffer) aggregation;
        partialResult[0] = accum.counts;
        partialResult[1] = accum.sums;

        // debug("doTerminatePartial exit, partialResult " + Arrays.toString(partialResult));
    }

    @Override
    protected Object doTerminate(AggregationBuffer aggregation) {
        // debug("doTerminate, accum " + aggregation.toString());

        MapFloatAverageBuffer accum = (MapFloatAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) {
            // debug("doTerminate, buffer is null, return null");
            return null;
        }
        if (accum.counts.isEmpty() || accum.sums.isEmpty()) {
            // debug("doTerminate, buffer is empty, return " + accum.sums);
            return accum.sums;
        }

        Map<String, Float> avg = mapProcessor.computeAverage(accum.counts, accum.sums);

        // debug("doTerminate, result " + avg);
        return avg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doIterate(AggregationBuffer aggregation, Map<String, ?> parameter) {
        // expects non-empty parameter
        // debug("doIterate enter, accum " + aggregation.toString() + "; parameter " + parameter);

        MapFloatAverageBuffer accum = (MapFloatAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
        mapProcessor.addItem(accum.counts, accum.sums, (Map<String, Float>) parameter);

        // debug("doIterate exit, accum " + aggregation.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doMerge(AggregationBuffer aggregation, Map<String, Long> partialCount, Map<String, ?> partialSum) {
        // debug("doMerge enter, accum " + aggregation.toString() + "; partialCount " + partialCount + "; partialSum " + partialSum);

        if (partialCount != null && partialSum != null) {
            MapFloatAverageBuffer accum = (MapFloatAverageBuffer) aggregation;
            if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
            mapProcessor.mergeBuffers(accum.counts, accum.sums, partialCount, (Map<String, Float>) partialSum);
        }

        // debug("doMerge exit, accum " + aggregation.toString());
    }

}
