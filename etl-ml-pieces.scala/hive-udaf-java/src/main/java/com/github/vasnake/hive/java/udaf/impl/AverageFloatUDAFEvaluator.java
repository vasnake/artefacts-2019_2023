/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.impl;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("deprecation")
public class AverageFloatUDAFEvaluator extends GenericUDAFEvaluator {
    // reference implementation, see https://github.com/apache/hive/blob/rel/release-2.1.1/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFAverage.java#L58

    protected transient static Logger LOG = LoggerFactory.getLogger(AverageFloatUDAFEvaluator.class.getName());
    protected static void debug(String msg) { LOG.debug(msg); }
    protected boolean warned = false; // ignore `iterate` exceptions but first

    // data setup performed in `init` method

    // For PARTIAL1 and COMPLETE
    protected transient PrimitiveObjectInspector inputOI; // JavaFloatObjectInspector

    // For PARTIAL2 and FINAL
    protected transient StructObjectInspector soi;
    protected transient StructField countField;
    protected transient StructField sumField;
    protected transient LongObjectInspector countFieldOI;
    protected transient ObjectInspector sumFieldOI; // JavaFloatObjectInspector

    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;

    // type dependant

    protected static class AverageAggregationBuffer implements AggregationBuffer {
        protected long count = 0;
        protected Float sum = 0f;

        public boolean isNull() {
            return (count == 0 && !sum.isNaN());
        }

        @Override public String toString() {
            return "AverageAggregationBuffer{" + "count=" + count + ", sum=" + sum + '}';
        }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        AverageAggregationBuffer buff = new AverageAggregationBuffer();
        reset(buff);
        return buff;
    }

    protected void doReset(AverageAggregationBuffer aggregation) throws HiveException {
        aggregation.count = 0;
        aggregation.sum = 0f;
    }

    protected ObjectInspector getInputFieldJavaObjectInspector() { return PrimitiveObjectInspectorFactory.javaFloatObjectInspector; }
    protected ObjectInspector getSumFieldWritableObjectInspector() { return PrimitiveObjectInspectorFactory.writableFloatObjectInspector; }

    protected void doTerminatePartial(AverageAggregationBuffer aggregation) {
        // TODO: get rid of Writable wrappers
        ((LongWritable) partialResult[0]).set(aggregation.count);
        if(partialResult[1] == null) {
            partialResult[1] = new FloatWritable(aggregation.sum);
        } else {
            ((FloatWritable) partialResult[1]).set(aggregation.sum);
        }
        // debug("doTerminatePartial exit, partialResult " + Arrays.toString(partialResult));
    }

    protected Object doTerminate(AverageAggregationBuffer aggregation) {
        // debug("doTerminate, accum " + aggregation.toString());

        if(aggregation.count == 0) {
            if (aggregation.isNull()) {
                return null;
            } else {
                return new FloatWritable(Float.NaN);
            }
        } else {
            FloatWritable result = new FloatWritable(aggregation.sum / aggregation.count);
            return result;
        }
    }

    protected void doIterate(AverageAggregationBuffer aggregation, PrimitiveObjectInspector oi, Object parameter) {
        // expected not null values
        // nan processing: should ignore unless previous input is null
        // debug("doIterate enter, accum " + aggregation.toString() + "; OI " + oi + ", parameter " + parameter);

        float value = PrimitiveObjectInspectorUtils.getFloat(parameter, oi);
        if (Float.isNaN(value)) { // if accum is null: set accum to nan, ignore otherwise
            if (aggregation.isNull()) {
                // debug("doIterate, input is nan and sum is null, set sum to nan");
                aggregation.sum = value;
            } // else { debug("doIterate, input is nan and sum is not null, ignore"); }
        } else { // input is not nan
            if (aggregation.count == 0) aggregation.sum = value; // sum was nan or 0
            else aggregation.sum += value;
            aggregation.count++;
        }

        // debug("doIterate exit, accum " + aggregation.toString());
    }

    protected void doMerge(AverageAggregationBuffer aggregation, Long partialCount, ObjectInspector sumFieldOI, Object partialSum) {
        // debug("doMerge enter, accum " + aggregation.toString() + "; partialCount " + partialCount + "; sumOI " + sumFieldOI + ", partialSumValue " + partialSum);

        float ps = ((FloatObjectInspector)sumFieldOI).get(partialSum); // could be nan
        if (partialCount == 0) { // ps null or nan
            if (Float.isNaN(ps)) { // ps is nan, ignore if accum is not null
                if (aggregation.count == 0) { // accum is null or nan, ps is nan
                    aggregation.sum = ps;
                } // else: accum is good, ps is nan
            } // else: ps is null, ignore
        } else { // partial is good
            if (aggregation.sum.isNaN()) aggregation.sum = ps;
            else aggregation.sum += ps;
            aggregation.count += partialCount;
        }

        // debug("doMerge exit, accum " + aggregation.toString());
    }

    // generic

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
        // debug("init, mode " + mode + ", parameters " + Arrays.toString(parameters));

        assert (parameters.length == 1);
        super.init(mode, parameters);

        // init input
        if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
            inputOI = (PrimitiveObjectInspector) parameters[0];
            // debug("init, original input " + inputOI);
        } else {
            soi = (StructObjectInspector) parameters[0];
            // debug("init, input for merge, struct " + soi);
            countField = soi.getStructFieldRef("count");
            sumField = soi.getStructFieldRef("sum");
            countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
            sumFieldOI = sumField.getFieldObjectInspector();
            inputOI = (PrimitiveObjectInspector) getInputFieldJavaObjectInspector();
        }

        // init output
        if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
            partialResult = new Object[2];
            partialResult[0] = new LongWritable(0); // index 1 reserved for sum

            // The output of a partial aggregation is a struct containing ("count", "sum")
            ArrayList<ObjectInspector> fields = new ArrayList<ObjectInspector>();
            fields.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector); // count
            fields.add(getSumFieldWritableObjectInspector()); // sum

            ArrayList<String> names = new ArrayList<String>();
            names.add("count"); names.add("sum");

            // debug("init, partial output " + names + "; " + fields);
            return ObjectInspectorFactory.getStandardStructObjectInspector(names, fields);
        } else {
            // final
            // debug("init, final output " + getSumFieldWritableObjectInspector());
            return getSumFieldWritableObjectInspector();
        }
    }

    @Override
    public void reset(AggregationBuffer aggregation) throws HiveException {
        doReset((AverageAggregationBuffer)aggregation);
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregation) throws HiveException {
        doTerminatePartial((AverageAggregationBuffer) aggregation);
        return partialResult;
    }

    @Override
    public Object terminate(AggregationBuffer aggregation) throws HiveException {
        return doTerminate((AverageAggregationBuffer)aggregation);
    }

    @Override
    public void iterate(AggregationBuffer aggregation, Object[] parameters) throws HiveException {
        // debug("iterate, accum " + aggregation.toString() + "; parameters " + Arrays.toString(parameters));

        assert (parameters.length == 1);
        Object parameter = parameters[0];

        if (parameter != null) {
            AverageAggregationBuffer averageAggregation = (AverageAggregationBuffer) aggregation;
            try {
                doIterate(averageAggregation, inputOI, parameter);
            } catch (NumberFormatException e) {
                // debug("iterate, error " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
                if (!warned) {
                    warned = true;
                    LOG.warn("Ignoring similar exceptions: " + StringUtils.stringifyException(e));
                }
            }
        } // else { debug("iterate, parameter is null, ignore"); }
    }

    @Override
    public void merge(AggregationBuffer aggregation, Object partial) throws HiveException {
        // debug("merge, accum " + aggregation.toString() + "; partial " + partial);

        if (partial != null) {
            doMerge(
                    (AverageAggregationBuffer)aggregation,
                    countFieldOI.get(soi.getStructFieldData(partial, countField)),
                    sumFieldOI, soi.getStructFieldData(partial, sumField)
            );
        }
    }

}
