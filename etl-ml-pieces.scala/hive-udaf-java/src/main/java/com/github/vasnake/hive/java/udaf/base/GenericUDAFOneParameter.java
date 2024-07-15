/**
 * Created by vasnake@gmail.com on 2024-07-15
 */
package com.github.vasnake.hive.java.udaf.base;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
abstract public class GenericUDAFOneParameter extends AbstractGenericUDAFResolver {
    // reference:
    // https://github.com/apache/hive/search?q=extends+AbstractGenericUDAFResolver
    // https://github.com/apache/hive/blob/rel/release-2.1.1/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFMin.java

    protected static Logger LOG = LoggerFactory.getLogger(GenericUDAFOneParameter.class.getName());

    protected static void debug(String msg) {
        // vim spark/conf/log4j.properties
        // log4j.logger.com.github.vasnake=DEBUG
        LOG.debug(msg);
    }
    protected static void info(String msg) { LOG.info(msg); }
    protected static void error(String msg) { LOG.error(msg); }

    public GenericUDAFOneParameter() {
        super();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {
        if (tis.length == 1) {
            // debug("getEvaluator, one parameter, " + tis[0].toString());
            TypeInfo columnTypeInfo = tis[0];

            switch (columnTypeInfo.getCategory()) {
                case PRIMITIVE: {
                    // debug("getEvaluator, column data type: primitive");
                    return primitiveEvaluator((PrimitiveTypeInfo) columnTypeInfo);
                }
                case LIST: {
                    // debug("getEvaluator, column data type: list ");
                    return listEvaluator((ListTypeInfo) columnTypeInfo);
                }
                case MAP: {
                    // debug("getEvaluator, column data type: map");
                    return mapEvaluator((MapTypeInfo) columnTypeInfo);
                }
                default: {
                    LOG.error("getEvaluator, unknown column data type " + columnTypeInfo.getCategory() + "");
                    throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter.getEvaluator. Argument must by of Map or List or Primitive type");
                }
            }
        }
        else {
            LOG.error("getEvaluator, params.length != 1");
            throw new UDFArgumentTypeException(tis.length - 1, "GenericUDAFOneParameter.getEvaluator. Exactly one arguments is expected.");
        }
    }

    abstract protected GenericUDAFEvaluator primitiveEvaluator(PrimitiveTypeInfo columnTypeInfo) throws SemanticException;

    abstract protected GenericUDAFEvaluator mapEvaluator(MapTypeInfo mapType) throws SemanticException;

    abstract protected GenericUDAFEvaluator listEvaluator(ListTypeInfo listType) throws SemanticException;

    protected void assertPrimitive(PrimitiveTypeInfo typeInfo) throws SemanticException {
        switch (typeInfo.getPrimitiveCategory()) {
            case FLOAT:
            case DOUBLE:
                break;
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case STRING:
            case DATE:
            case TIMESTAMP:
            case DECIMAL:
            case VARCHAR:
            case CHAR: {
                String msg = "assertPrimitive, argument of primitive type must be of Double or Float type, got " + typeInfo.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
            default: {
                String msg = "assertPrimitive, unsupported primitive type " + typeInfo.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
        }
    }

    protected void assertList(ListTypeInfo columnTypeInfo) throws SemanticException {
        // TODO: use evaluators factory with mappings
        TypeInfo valueTypeInfo = columnTypeInfo.getListElementTypeInfo();
        if (valueTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            String msg = "assertList, list elements must be of primitive category, got " + valueTypeInfo.getCategory();
            error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
        }

        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) valueTypeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case FLOAT:
            case DOUBLE:
                break;
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case STRING:
            case DATE:
            case TIMESTAMP:
            case DECIMAL:
            case VARCHAR:
            case CHAR: {
                String msg = "assertList, list elements must be of Double or Float type, got " + primitiveTypeInfo.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
            default: {
                String msg = "assertList, unsupported list elements type " + primitiveTypeInfo.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
        }
    }

    protected void assertMap(MapTypeInfo columnTypeInfo) throws SemanticException {
        // TODO: use evaluators factory with mappings
        TypeInfo valueTypeInfo = columnTypeInfo.getMapValueTypeInfo();
        TypeInfo keyTypeInfo = columnTypeInfo.getMapKeyTypeInfo();
        if (valueTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            String msg = "assertMap, map values must be of primitive category, got " + valueTypeInfo.getCategory();
            error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
        }
        if (keyTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            String msg = "assertMap, map keys must be of primitive category, got " + keyTypeInfo.getCategory();
            error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
        }

        PrimitiveTypeInfo keyPTI = (PrimitiveTypeInfo) keyTypeInfo;
        if (keyPTI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            String msg = "assertMap, map keys must be of type String, got " + keyPTI.getPrimitiveCategory();
            error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
        }

        PrimitiveTypeInfo valuePTI = (PrimitiveTypeInfo) valueTypeInfo;
        switch (valuePTI.getPrimitiveCategory()) {
            case FLOAT:
            case DOUBLE:
                break;
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case STRING:
            case DATE:
            case TIMESTAMP:
            case DECIMAL:
            case VARCHAR:
            case CHAR: {
                String msg = "assertMap, map values must be of Double or Float type, got " + valuePTI.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
            default: {
                String msg = "assertMap, unsupported map value type " + valuePTI.getPrimitiveCategory();
                error(msg); throw new UDFArgumentTypeException(0, "GenericUDAFOneParameter." + msg);
            }
        }
    }

}
