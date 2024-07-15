/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.impl;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.github.vasnake.hive.java.udaf.base.GenericListAverageUDAFEvaluator;

@SuppressWarnings("deprecation")
public class AverageListFloatUDAFEvaluator extends GenericListAverageUDAFEvaluator {

    protected static void debug(String msg) { GenericListAverageUDAFEvaluator.debug("AverageListFloatUDAFEvaluator." + msg); }

    protected static class ListFloatAverageBuffer implements AggregationBuffer {
        // rules:
        // count=0 and sum=0: sum is null,
        // count=0 and sum.isNaN: sum is nan;
        // ignore null, ignore nan if sum is not null;
        // null collection: result is null,
        // null collection and empty collection: result is empty collection.
        protected ArrayList<Long> counts = null; // 0..max
        protected ArrayList<Float> sums = null; // 0, nan, any float, no null!

        protected void newEmptyBuffer() {
            counts = new ArrayList<>();
            sums = new ArrayList<>();
        }

        @Override public String toString() {
            return "ListFloatAverageBuffer{" + "counts=" + counts + ", sums=" + sums + '}';
        }
    }

    protected static class FloatListAverage extends GenericListAverageUDAFEvaluator.NumericListAverage<Float> {
        @Override public boolean isNaN(Float x) { return x.isNaN(); }
        @Override public Float sum(Float a, Float b) { return a + b; }
        @Override public Float div(Float a, Long b) { return a / b; }
        @Override public Float zero() { return 0f; }
    }

    private final static FloatListAverage listProcessor = new FloatListAverage();

    @Override protected AggregationBuffer newAverageAggregationBuffer() {
        return new ListFloatAverageBuffer();
    }

    @Override
    protected void doReset(AggregationBuffer aggregation) {
        // debug("doReset, accum " + aggregation.toString());

        ListFloatAverageBuffer accum = (ListFloatAverageBuffer) aggregation;
        if (accum.counts != null) accum.counts.clear();
        if (accum.sums != null) accum.sums.clear();

        accum.counts = null;
        accum.sums = null;
    }

    @Override
    protected ObjectInspector getInputFieldJavaObjectInspector() {
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaFloatObjectInspector
        );
    }

    @Override
    protected ObjectInspector getSumFieldWritableObjectInspector() {
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaFloatObjectInspector
        );
    }

    @Override
    protected void doTerminatePartial(AggregationBuffer aggregation) {
        // debug("doTerminatePartial enter, accum " + aggregation.toString());

        ListFloatAverageBuffer accum = (ListFloatAverageBuffer) aggregation;
        partialResult[0] = accum.counts;
        partialResult[1] = accum.sums;

        // debug("doTerminatePartial exit, partialResult " + Arrays.toString(partialResult));
    }

    @Override
    protected Object doTerminate(AggregationBuffer aggregation) {
        // debug("doTerminate, accum " + aggregation.toString());

        ListFloatAverageBuffer accum = (ListFloatAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) {
            // debug("doTerminate, buffer is null, return null");
            return null;
        }
        if (accum.counts.isEmpty() || accum.sums.isEmpty()) {
            // debug("doTerminate, buffer is empty, return " + accum.sums);
            return accum.sums;
        }

        List<Float> avg = listProcessor.computeAverage(accum.counts, accum.sums);

        // debug("doTerminate, result " + avg);
        return avg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doIterate(AggregationBuffer aggregation, List<?> parameter) {
        // expects non-empty parameter
        // debug("doIterate enter, accum " + aggregation.toString() + "; parameter " + parameter);

        ListFloatAverageBuffer accum = (ListFloatAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
        listProcessor.addItem(accum.counts, accum.sums, (List<Float>) parameter);

        // debug("doIterate exit, accum " + aggregation.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doMerge(AggregationBuffer aggregation, List<Long> partialCount, List<?> partialSum) {
        // debug("doMerge enter, accum " + aggregation.toString() + "; partialCount " + partialCount + "; partialSum " + partialSum);

        if (partialCount != null && partialSum != null) {
            ListFloatAverageBuffer accum = (ListFloatAverageBuffer) aggregation;
            if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
            listProcessor.mergeBuffers(accum.counts, accum.sums, partialCount, (List<Float>) partialSum);
        }

        // debug("doMerge exit, accum " + aggregation.toString());
    }

}
