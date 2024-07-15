/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf;

import com.github.vasnake.hive.java.udaf.base.GenericUDAFOneParameter;
import com.github.vasnake.hive.java.udaf.impl.*;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL `avg` function that should work for column types:
 * primitive(bool, byte, short, int, long, float, double, string, date, timestamp, binary, decimal, varchar, char);
 * list[double|float],
 * map[string, double|float].
 *
 * N.B: for primitive types implemented only (float, double).
 *
 * To register function, use sql expression like this:
 <pre>
 drop temporary function if exists avg_features
 create temporary function avg_features as "com.github.vasnake.hive.java.udaf.GenericAvgUDAF"
 </pre>
 */

@Description(
        name = "avg_features",
        value = "_FUNC_(expr) - Returns the average value of expr." +
                "expr could be a column of primitive type or list[double|float] or map[string, double|float]."
)
@SuppressWarnings("deprecation")
public class GenericAvgUDAF extends GenericUDAFOneParameter {

    protected static Logger LOG = LoggerFactory.getLogger(GenericAvgUDAF.class.getName());
    protected static void debug(String msg) {
        // vim spark/conf/log4j.properties
        // log4j.logger.com.mrg.dm=DEBUG
        LOG.debug(msg);
    }
    protected static void info(String msg) {
        LOG.info(msg);
    }
    protected static void error(String msg) {
        LOG.error(msg);
    }

    public GenericAvgUDAF() {
        super();
    }

    @Override
    protected GenericUDAFEvaluator primitiveEvaluator(PrimitiveTypeInfo columnTypeInfo) throws SemanticException {
        // debug("primitiveEvaluator, assert data type " + columnTypeInfo + " ...");
        assertPrimitive(columnTypeInfo);
        switch (columnTypeInfo.getPrimitiveCategory()) {
            case FLOAT: return new AvgFloat();
            case DOUBLE: return new AvgDouble();
            default: throw new UDFArgumentTypeException(0, "GenericAvgUDAF.primitiveEvaluator, `assertPrimitive` has failed to check types");
        }
    }

    @Override
    protected GenericUDAFEvaluator listEvaluator(ListTypeInfo columnTypeInfo) throws SemanticException {
        // debug("listEvaluator, assert data type " + columnTypeInfo + " ...");
        assertList(columnTypeInfo);
        TypeInfo valueTypeInfo = columnTypeInfo.getListElementTypeInfo();
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) valueTypeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case FLOAT: return new AvgListFloat();
            case DOUBLE: return new AvgListDouble();
            default: throw new UDFArgumentTypeException(0, "GenericAvgUDAF.listEvaluator, `assertList` has failed to check types");
        }
    }

    @Override
    protected GenericUDAFEvaluator mapEvaluator(MapTypeInfo columnTypeInfo) throws SemanticException {
        assertMap(columnTypeInfo);
        TypeInfo valueTypeInfo = columnTypeInfo.getMapValueTypeInfo();
        PrimitiveTypeInfo valuePTI = (PrimitiveTypeInfo) valueTypeInfo;
        switch (valuePTI.getPrimitiveCategory()) {
            case FLOAT: return new AvgMapFloat();
            case DOUBLE: return new AvgMapDouble();
            default: throw new UDFArgumentTypeException(0, "GenericAvgUDAF.mapEvaluator, `assertMap` has failed to check types");
        }
    }

    public static class AvgMapDouble extends AverageMapDoubleUDAFEvaluator {
        protected static void debug(String msg) { AverageListFloatUDAFEvaluator.debug("AvgMapDouble." + msg); }
    }

    public static class AvgMapFloat extends AverageMapFloatUDAFEvaluator {
        protected static void debug(String msg) { AverageListFloatUDAFEvaluator.debug("AvgMapFloat." + msg); }
    }

    public static class AvgListDouble extends AverageListDoubleUDAFEvaluator {
        protected static void debug(String msg) { AverageListFloatUDAFEvaluator.debug("AvgListDouble." + msg); }
    }

    public static class AvgListFloat extends AverageListFloatUDAFEvaluator {
        protected static void debug(String msg) { AverageListFloatUDAFEvaluator.debug("AvgListFloat." + msg); }
    }

    public static class AvgDouble extends AverageDoubleUDAFEvaluator {
        protected static void debug(String msg) { AverageDoubleUDAFEvaluator.debug("AvgDouble." + msg); }
    }

    public static class AvgFloat extends AverageFloatUDAFEvaluator {
        protected static void debug(String msg) { AverageFloatUDAFEvaluator.debug("AvgFloat." + msg); }
    }

}
