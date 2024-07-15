/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.base;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("deprecation")
public abstract class GenericPrimitiveAverageUDAFEvaluator extends GenericUDAFEvaluator {

    protected transient static Logger LOG = LoggerFactory.getLogger(GenericPrimitiveAverageUDAFEvaluator.class.getName());
    protected static void debug(String msg) { LOG.debug(msg); }
    protected transient boolean warned = false; // ignore `iterate` exceptions but first

    // data setup will be performed in `init` method according to `mode` parameter

    // For PARTIAL1 and COMPLETE
    protected transient PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL
    protected transient StructObjectInspector soi;
    protected transient StructField countField;
    protected transient StructField sumField;
    protected transient LongObjectInspector countFieldOI;
    protected transient PrimitiveObjectInspector sumFieldOI;

    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;

    abstract protected AggregationBuffer newAverageAggregationBuffer();
    abstract protected void doReset(AggregationBuffer aggregation);
    abstract protected ObjectInspector getInputFieldJavaObjectInspector();
    abstract protected ObjectInspector getSumFieldWritableObjectInspector();
    abstract protected void doTerminatePartial(AggregationBuffer aggregation);
    abstract protected Object doTerminate(AggregationBuffer aggregation);
    abstract protected void doIterate(AggregationBuffer aggregation, ObjectInspector oi, Object parameter);
    abstract protected void doMerge(AggregationBuffer aggregation, Long partialCount, ObjectInspector sumFieldOI, Object partialSum);

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        // debug("getNewAggregationBuffer");

        AggregationBuffer result = newAverageAggregationBuffer();
        reset(result);
        return result;
    }

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
            sumFieldOI = (PrimitiveObjectInspector) sumField.getFieldObjectInspector();
            inputOI = (PrimitiveObjectInspector) getInputFieldJavaObjectInspector();
        }

        // init output
        if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
            partialResult = new Object[2];
            partialResult[0] = new LongWritable(0); // index 1 reserved for sum

            // The output of a partial aggregation is a struct containing ("count", "sum")
            ArrayList<ObjectInspector> fields = new ArrayList<>();
            fields.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector); // count
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

    @Override
    public void reset(AggregationBuffer aggregation) throws HiveException {
        doReset(aggregation);
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregation) throws HiveException {
        doTerminatePartial(aggregation);
        return partialResult;
    }

    @Override
    public Object terminate(AggregationBuffer aggregation) throws HiveException {
        return doTerminate(aggregation);
    }

    @Override
    public void iterate(AggregationBuffer aggregation, Object[] parameters) throws HiveException {
        // debug("iterate, accum " + aggregation.toString() + "; parameters " + Arrays.toString(parameters));

        assert (parameters.length == 1);
        Object parameter = parameters[0];

        if (parameter != null) {
            try {
                doIterate(aggregation, inputOI, parameter);
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
                    aggregation,
                    countFieldOI.get(soi.getStructFieldData(partial, countField)),
                    sumFieldOI, soi.getStructFieldData(partial, sumField)
            );
        } // else { debug("merge, partial accum is null, skip");}
    }

}
