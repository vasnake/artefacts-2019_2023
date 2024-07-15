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
public class AverageListDoubleUDAFEvaluator extends GenericListAverageUDAFEvaluator {

    protected static void debug(String msg) { GenericListAverageUDAFEvaluator.debug("AverageListDoubleUDAFEvaluator." + msg); }

    protected static class ListDoubleAverageBuffer implements AggregationBuffer {
        // rules:
        // count=0 and sum=0: sum is null,
        // count=0 and sum.isNaN: sum is nan;
        // ignore null, ignore nan if sum is not null;
        // null collection: result is null,
        // null collection and empty collection: result is empty collection.
        protected ArrayList<Long> counts = null; // 0..max
        protected ArrayList<Double> sums = null; // 0, nan, any double, no null!

        protected void newEmptyBuffer() {
            counts = new ArrayList<>();
            sums = new ArrayList<>();
        }

        @Override public String toString() {
            return "ListDoubleAverageBuffer{" + "counts=" + counts + ", sums=" + sums + '}';
        }
    }

    protected static class DoubleListAverage extends GenericListAverageUDAFEvaluator.NumericListAverage<Double> {
        @Override public boolean isNaN(Double x) { return x.isNaN(); }
        @Override public Double sum(Double a, Double b) { return a + b; }
        @Override public Double div(Double a, Long b) { return a / b; }
        @Override public Double zero() { return 0d; }
    }

    private final static DoubleListAverage listProcessor = new DoubleListAverage();

    @Override protected AggregationBuffer newAverageAggregationBuffer() {
        return new ListDoubleAverageBuffer();
    }

    @Override
    protected void doReset(AggregationBuffer aggregation) {
        // debug("doReset, accum " + aggregation.toString());

        ListDoubleAverageBuffer accum = (ListDoubleAverageBuffer) aggregation;
        if (accum.counts != null) accum.counts.clear();
        if (accum.sums != null) accum.sums.clear();

        accum.counts = null;
        accum.sums = null;
    }

    @Override
    protected ObjectInspector getInputFieldJavaObjectInspector() {
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        );
    }

    @Override
    protected ObjectInspector getSumFieldWritableObjectInspector() {
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        );
    }

    @Override
    protected void doTerminatePartial(AggregationBuffer aggregation) {
        // debug("doTerminatePartial enter, accum " + aggregation.toString());

        ListDoubleAverageBuffer accum = (ListDoubleAverageBuffer) aggregation;
        partialResult[0] = accum.counts;
        partialResult[1] = accum.sums;

        // debug("doTerminatePartial exit, partialResult " + Arrays.toString(partialResult));
    }

    @Override
    protected Object doTerminate(AggregationBuffer aggregation) {
        // debug("doTerminate, accum " + aggregation.toString());

        ListDoubleAverageBuffer accum = (ListDoubleAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) {
            // debug("doTerminate, buffer is null, return null");
            return null;
        }
        if (accum.counts.isEmpty() || accum.sums.isEmpty()) {
            // debug("doTerminate, buffer is empty, return " + accum.sums);
            return accum.sums;
        }

        List<Double> avg = listProcessor.computeAverage(accum.counts, accum.sums);
        // debug("doTerminate, result " + avg);
        return avg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doIterate(AggregationBuffer aggregation, List<?> parameter) {
        // expects non-empty parameter
        // debug("doIterate enter, accum " + aggregation.toString() + "; parameter " + parameter);

        ListDoubleAverageBuffer accum = (ListDoubleAverageBuffer) aggregation;
        if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
        listProcessor.addItem(accum.counts, accum.sums, (List<Double>) parameter);

        // debug("doIterate exit, accum " + aggregation.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doMerge(AggregationBuffer aggregation, List<Long> partialCount, List<?> partialSum) {
        // debug("doMerge enter, accum " + aggregation.toString() + "; partialCount " + partialCount + "; partialSum " + partialSum);

        if (partialCount != null && partialSum != null) {
            ListDoubleAverageBuffer accum = (ListDoubleAverageBuffer) aggregation;
            if (accum.counts == null || accum.sums == null) accum.newEmptyBuffer();
            listProcessor.mergeBuffers(accum.counts, accum.sums, partialCount, (List<Double>) partialSum);
        }

        // debug("doMerge exit, accum " + aggregation.toString());
    }

}
