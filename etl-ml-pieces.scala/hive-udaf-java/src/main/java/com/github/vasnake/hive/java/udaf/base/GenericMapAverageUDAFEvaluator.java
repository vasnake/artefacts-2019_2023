/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.base;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public abstract class GenericMapAverageUDAFEvaluator extends GenericPrimitiveAverageUDAFEvaluator {
    // https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy#GenericUDAFCaseStudy-Writingtheevaluator

    protected transient static Logger LOG = LoggerFactory.getLogger(GenericMapAverageUDAFEvaluator.class.getName());
    protected static void debug(String msg) { LOG.debug(msg); }

    protected transient MapObjectInspector inputOI;
    protected transient MapObjectInspector countFieldOI;
    protected transient MapObjectInspector sumFieldOI;

    abstract protected void doIterate(AggregationBuffer aggregation, Map<String, ?> parameter);
    abstract protected void doMerge(AggregationBuffer aggregation, Map<String, Long> partialCount, Map<String, ?> partialSum);

    @Override protected void doIterate(AggregationBuffer aggregation, ObjectInspector oi, Object parameter) { throw new UnsupportedOperationException("MapAverage.doIterate must take Map parameter"); }
    @Override protected void doMerge(AggregationBuffer aggregation, Long partialCount, ObjectInspector sumFieldOI, Object partialSum) { throw new UnsupportedOperationException("MapAverage.doMerge must take Map parameters"); }

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
        // debug("init, mode " + mode + ", parameters " + Arrays.toString(parameters));
        assert (parameters.length == 1);

        // init input
        if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
            inputOI = (MapObjectInspector) parameters[0];
            // debug("init, original input " + inputOI);
        } else {
            soi = (StructObjectInspector) parameters[0];
            // debug("init, input for merge, struct " + soi);
            countField = soi.getStructFieldRef("count");
            sumField = soi.getStructFieldRef("sum");
            countFieldOI = (MapObjectInspector) countField.getFieldObjectInspector();
            sumFieldOI = (MapObjectInspector) sumField.getFieldObjectInspector();
            inputOI = (MapObjectInspector) getInputFieldJavaObjectInspector();
        }

        // init output
        if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
            partialResult = new Object[2]; // 0: counts, 1: sums

            // The output of a partial aggregation is a struct containing ("count", "sum")
            ArrayList<ObjectInspector> fields = new ArrayList<>();
            fields.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaLongObjectInspector)
            ); // count
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
        // debug("iterate, accum " + aggregation.toString() + "; parameters " + Arrays.toString(parameters));

        assert (parameters.length == 1);
        Object parameter = parameters[0];

        if (parameter != null) {
            Map<String, Object> input = (Map<String, Object>) inputOI.getMap(parameter);

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
        // debug("merge, accum " + aggregation.toString() + "; partial " + partial);

        if (partial != null) {
            Map<String, Long> counts = (Map<String, Long>) countFieldOI.getMap(soi.getStructFieldData(partial, countField));

            if (counts != null) {
                doMerge(
                        aggregation,
                        counts,
                        (Map<String, ?>) sumFieldOI.getMap(soi.getStructFieldData(partial, sumField))
                );
            } // else { debug("merge, partial accum is null, skip"); }

        } // else { debug("merge, partial structure is null, skip"); }

        // debug("merge exit, accum " + aggregation);
    }

    abstract public static class NumericMapAverage<T> {
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

        public Map<String, T> computeAverage(Map<String, Long> accumCounts, Map<String, T> accumSums) {
            Map<String, T> avg = new HashMap<>();
            if (accumCounts.isEmpty()) return avg;

            Long count;
            T sum;
            for (String key: accumCounts.keySet()) {
                count = accumCounts.get(key);
                sum = accumSums.get(key);

                if (count == 0) { // null or nan
                    if (isNull(count, sum)) {
                        avg.put(key, null);
                    } else {
                        avg.put(key, sum);
                    }
                }
                else {
                    avg.put(key, div(sum, count));
                }

            }

            return avg;
        }

        public void addItem(Map<String, Long> accumCounts, Map<String, T> accumSums, Map<String, T> input) {
            // debug("addItem enter, accum counts " + accumCounts + ", accum sums " + accumSums + ", input " + input);

            T inputItem, accumItem;
            Long count;
            for (String key: input.keySet()) {
                inputItem = input.get(key);

                if (accumCounts.containsKey(key)) { // merge
                    count = accumCounts.get(key);
                    accumItem = accumSums.get(key);

                    if (inputItem != null && !isNaN(inputItem)) { // input is good
                        if (isNull(count, accumItem) || isNaN(accumItem)) { // accum is no good, just set it to input
                            accumSums.put(key, inputItem);
                            accumCounts.put(key, 1L);
                        } else { // input is good, accum is good
                            accumSums.put(key, sum(inputItem, accumItem));
                            accumCounts.put(key, count + 1);
                        }
                    } else { // input is null or nan, check accum for null
                        if (inputItem != null && isNull(count, accumItem)) { // input is nan and accum is null, just set it to input value
                            accumSums.put(key, inputItem);
                        }
                    }

                } else { // no key in accum, put new value
                    if (inputItem == null) {
                        accumCounts.put(key, 0L);
                        accumSums.put(key, zero());
                    } else if (isNaN(inputItem)) {
                        accumCounts.put(key, 0L);
                        accumSums.put(key, inputItem);
                    } else {
                        accumSums.put(key, inputItem);
                        accumCounts.put(key, 1L);
                    }
                }

            }

            // debug("addItem exit, accum counts " + accumCounts + ", accum sums " + accumSums);
        }

        public void mergeBuffers(Map<String, Long> accumCounts, Map<String, T> accumSums, Map<String, Long> inputCounts, Map<String, T> inputSums) {
            T inputItem, accumItem;
            Long inputCount, accumCount;
            for (String key: inputCounts.keySet()) {
                inputItem = inputSums.get(key);
                inputCount = inputCounts.get(key);

                if (accumCounts.containsKey(key)) { // merge
                    accumItem = accumSums.get(key);
                    accumCount = accumCounts.get(key);

                    if (!isNull(inputCount, inputItem) && !isNaN(inputItem)) { // input is good
                        if (isNull(accumCount, accumItem) || isNaN(accumItem)) { // accum is no good, set to input
                            accumSums.put(key, inputItem);
                            accumCounts.put(key, inputCount);
                        } else { // input is good, accum is good, add
                            accumSums.put(key, sum(inputItem, accumItem));
                            accumCounts.put(key, inputCount + accumCount);
                        }
                    } else { // input is null or nan, ignore if accum is not null
                        if (isNaN(inputItem) && isNull(accumCount, accumItem)) {
                            accumSums.put(key, inputItem);
                        }
                    }

                } else { // no key in accum, just put new value
                    accumSums.put(key, inputItem);
                    accumCounts.put(key, inputCount);
                }
            }

        }
    }

}
