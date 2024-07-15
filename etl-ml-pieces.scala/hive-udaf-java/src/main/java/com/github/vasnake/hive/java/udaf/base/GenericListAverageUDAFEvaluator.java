/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.base;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("deprecation")
public abstract class GenericListAverageUDAFEvaluator extends GenericPrimitiveAverageUDAFEvaluator {
    // https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy#GenericUDAFCaseStudy-Writingtheevaluator

    protected transient static Logger LOG = LoggerFactory.getLogger(GenericListAverageUDAFEvaluator.class.getName());
    protected static void debug(String msg) { LOG.debug(msg); }

    protected transient ListObjectInspector inputOI;
    protected transient ListObjectInspector countFieldOI;
    protected transient ListObjectInspector sumFieldOI;

    abstract protected void doIterate(AggregationBuffer aggregation, List<?> parameter);
    abstract protected void doMerge(AggregationBuffer aggregation, List<Long> partialCount, List<?> partialSum);

    @Override
    protected void doIterate(AggregationBuffer aggregation, ObjectInspector oi, Object parameter) { throw new UnsupportedOperationException("ListAverage.doIterate must take List parameter"); }
    @Override
    protected void doMerge(AggregationBuffer aggregation, Long partialCount, ObjectInspector sumFieldOI, Object partialSum) { throw new UnsupportedOperationException("ListAverage.doMerge must take List parameters"); }

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
        // debug("init, mode " + mode + ", parameters " + Arrays.toString(parameters));
        assert (parameters.length == 1);

        // init input
        if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
            inputOI = (ListObjectInspector) parameters[0];
            // debug("init, original input " + inputOI);
        } else {
            soi = (StructObjectInspector) parameters[0];
            // debug("init, input for merge, struct " + soi);
            countField = soi.getStructFieldRef("count");
            sumField = soi.getStructFieldRef("sum");
            countFieldOI = (ListObjectInspector) countField.getFieldObjectInspector();
            sumFieldOI = (ListObjectInspector) sumField.getFieldObjectInspector();
            inputOI = (ListObjectInspector) getInputFieldJavaObjectInspector();
        }

        // init output
        if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
            partialResult = new Object[2]; // 0: counts, 1: sums

            // The output of a partial aggregation is a struct containing ("count", "sum")
            ArrayList<ObjectInspector> fields = new ArrayList<>();
            fields.add(ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaLongObjectInspector)); // count
            fields.add(getSumFieldWritableObjectInspector()); // sum

            ArrayList<String> names = new ArrayList<>();
            names.add("count"); names.add("sum");

            // debug("init, partial output " + names + "; " + fields);
            return ObjectInspectorFactory.getStandardStructObjectInspector(names, fields);
        } else {
            // final
            // debug("init, final output " + getSumFieldWritableObjectInspector());
            return getSumFieldWritableObjectInspector();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(AggregationBuffer aggregation, Object[] parameters) throws HiveException {
        // debug("iterate enter, accum " + aggregation.toString() + "; parameters " + Arrays.toString(parameters));

        assert (parameters.length == 1);
        Object parameter = parameters[0];

        if (parameter != null) {
            List<Object> input = (List<Object>) inputOI.getList(parameter);

            try {
                doIterate(aggregation, input);
            } catch (NumberFormatException e) {
                // debug("iterate, error " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
                if (!warned) {
                    warned = true;
                    LOG.warn("Ignoring similar exceptions: " + StringUtils.stringifyException(e));
                }
            }
        } // else { debug("iterate, parameter is null, ignore"); }

        // debug("iterate exit, accum " + aggregation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void merge(AggregationBuffer aggregation, Object partial) throws HiveException {
        // debug("merge enter, accum " + aggregation.toString() + "; partial " + partial);

        if (partial != null) {
            List<Long> counts = (List<Long>) countFieldOI.getList(soi.getStructFieldData(partial, countField));

            if (counts != null) {
                doMerge(
                        aggregation,
                        counts,
                        sumFieldOI.getList(soi.getStructFieldData(partial, sumField))
                );
            } // else { debug("merge, partial accum is null, skip"); }

        } // else { debug("merge, partial structure is null, skip"); }

        // debug("merge exit, accum " + aggregation);
    }

    abstract public static class NumericListAverage<T> {
        // rules:
        // count=0 and sum=0: sum is null,
        // count=0 and sum.isNaN: sum is nan;
        // ignore null; ignore nan if sum is not null;
        // null collection: result is null,
        // null collection and empty collection: result is empty collection.

        public abstract boolean isNaN(T x);
        public abstract T sum(T a, T b);
        public abstract T div(T a, Long b);
        public abstract T zero();

        public boolean isNull(Long count, T sum) {
            return (count == 0 && !isNaN(sum));
        }

        public List<T> computeAverage(List<Long> accumCounts, List<T> accumSums) {
            ArrayList<T> avg = new ArrayList<>(accumCounts.size());
            if(accumCounts.isEmpty()) return avg;

            int i = 0;
            for (Long count : accumCounts) {
                T sum = accumSums.get(i);

                if (count == 0) { // null or nan
                    if (isNull(count, sum)) {
                        avg.add(null);
                    } else {
                        avg.add(sum);
                    }
                }
                else {
                    avg.add(div(sum, count));
                }

                i++;
            }

            return avg;
        }

        public void addItem(List<Long> accumCounts, List<T> accumSums, List<T> input) {
            // debug("addItem enter, accum counts " + accumCounts + ", accum sums " + accumSums + ", input " + input);

            int accumSize = accumCounts.size();
            int i = 0;
            T accumItem;
            Long count;
            for (T inputItem : input) {
                if (i < accumSize) { // merge
                    accumItem = accumSums.get(i);
                    count = accumCounts.get(i);

                    if (inputItem != null && !isNaN(inputItem)) { // input is good
                        if (isNull(count, accumItem) || isNaN(accumItem)) { // accum is no good, just set it to input
                            accumSums.set(i, inputItem);
                            accumCounts.set(i, 1L);
                        } else { // input is good, accum is good
                            accumSums.set(i, sum(inputItem, accumItem));
                            accumCounts.set(i, count + 1);
                        }
                    } else { // input is null or nan, check accum for null
                        if (inputItem != null && isNull(count, accumItem)) { // input is nan and accum is null, just set it to input value
                            accumSums.set(i, inputItem);
                        }
                    }

                } else { // accum is shorter than input, append input value
                    if (inputItem == null) {
                        accumCounts.add(0L);
                        accumSums.add(zero());
                    } else if (isNaN(inputItem)) {
                        accumCounts.add(0L);
                        accumSums.add(inputItem);
                    } else {
                        accumSums.add(inputItem);
                        accumCounts.add(1L);
                    }
                    accumSize++;
                }

                i++;
            }

            // debug("addItem exit, accum counts " + accumCounts + ", accum sums " + accumSums);
        }

        public void mergeBuffers(List<Long> accumCounts, List<T> accumSums, List<Long> inputCounts, List<T> inputSums) {
            int accumSize = accumCounts.size();
            int i = 0;
            Long accumCount;
            Long inputCount;
            T accumItem;
            for (T inputItem: inputSums) {
                inputCount = inputCounts.get(i);

                if (i < accumSize) { // merge
                    accumCount = accumCounts.get(i);
                    accumItem = accumSums.get(i);

                    if (!isNull(inputCount, inputItem) && !isNaN(inputItem)) { // input is good
                        if (isNull(accumCount, accumItem) || isNaN(accumItem)) { // accum is no good, set to input
                            accumSums.set(i, inputItem);
                            accumCounts.set(i, inputCount);
                        } else { // input is good, accum is good, add
                            accumSums.set(i, sum(inputItem, accumItem));
                            accumCounts.set(i, inputCount + accumCount);
                        }
                    } else { // input is null or nan, ignore if accum is not null
                        if (isNaN(inputItem) && isNull(accumCount, accumItem)) {
                            accumSums.set(i, inputItem);
                        }
                    }

                } else { // accum is shorter than input, append input value
                    accumSums.add(inputItem);
                    accumCounts.add(inputCount);
                    accumSize++;
                }

                i++;
            }
        }
    }

}
