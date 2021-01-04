package org.opencb.oskar.spark.commons.converters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.opencb.biodata.tools.commons.Converter;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 06/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class RowToAvroConverter<T extends GenericRecord> implements Converter<Row, T> {

    public static <T extends GenericRecord> T convert(GenericRowWithSchema row, T empty) {
        return convert(row, row.schema(), empty, empty.getSchema());
    }

    public static <T extends GenericRecord> T convert(Row row, StructType dataType, T empty) {
        return convert(row, dataType, empty, empty.getSchema());
    }

    public static <T extends GenericRecord> T convert(Row row, StructType dataType, T empty, Schema schema) {
        int i = 0;
        for (StructField structField : dataType.fields()) {
            Object o = row.get(i);
            Schema subSchema = schema.getFields().get(i).schema();
            o = convert(o, structField.dataType(), subSchema);
            empty.put(i, o);
            i++;
        }
        return empty;
    }

    public static Schema getUnionSubType(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            // Assume that all unions are between null and something else
            for (Schema subSchema : schema.getTypes()) {
                if (subSchema.getType() != Schema.Type.NULL) {
                    return subSchema;
                }
            }
        } else {
            return schema;
        }
        throw new IllegalArgumentException("Typed schema not found!");
    }

    public static <T> T convert(Object o, DataType dataType, Schema schema) {
        try {
            if (o == null) {
                return null;
            }
            // In case of UNION, get subtype
            schema = getUnionSubType(schema);
            if (dataType instanceof ArrayType) {
                List<Object> list = new ArrayList<>(((WrappedArray) o).length());

                Schema elementType = schema.getElementType();

                for (int i1 = 0; i1 < ((WrappedArray) o).length(); i1++) {
                    Object elem = ((WrappedArray) o).apply(i1);

                    elem = convert(elem, ((ArrayType) dataType).elementType(), elementType);
                    list.add(elem);
                }
                o = list;
            } else if (dataType instanceof MapType) {
                Map<?, ?> map = scala.collection.JavaConversions.mapAsJavaMap((scala.collection.Map) o);
                Map<Object, Object> avroMap = new HashMap<>(map.size());

                Schema valueType = schema.getValueType();

                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object value = convert(entry.getValue(), ((MapType) dataType).valueType(), valueType);
                    avroMap.put(entry.getKey(), value);
                }

                o = avroMap;
            } else if (dataType instanceof StructType) {
                GenericRecord empty;
                try {
                    empty = (GenericRecord) Class.forName(schema.getFullName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                StructType structType = (StructType) dataType;
                Row row = (Row) o;
                if (structType.fields().length == row.length()) {
                    int i = 0;
                    for (StructField structField : structType.fields()) {
                        Object elem = row.get(i);
                        Schema subSchema = schema.getFields().get(i).schema();
                        elem = convert(elem, structField.dataType(), subSchema);
                        empty.put(i, elem);
                        i++;
                    }
                } else {
                    GenericRowWithSchema rowWithSchema = ((GenericRowWithSchema) row);

                    StructField[] fields = structType.fields();
                    for (int i = 0; i < fields.length; i++) {
                        StructField structField = fields[i];
                        Object elem;
                        try {
                            int rowIdx = rowWithSchema.fieldIndex(structField.name());
                            elem = row.get(rowIdx);
                        } catch (IllegalArgumentException e) {
                            continue;
                        }
                        Schema subSchema = schema.getFields().get(i).schema();
                        elem = convert(elem, structField.dataType(), subSchema);
                        empty.put(i, elem);
                    }
                }

                o = empty;
            } else if (schema.getType().equals(Schema.Type.ENUM)) {
                try {
                    Class<Enum> enumClass = (Class<Enum>) Class.forName(schema.getFullName());
                    o = Enum.valueOf(enumClass, o.toString());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            System.out.println("o = " + o);
            System.out.println("dataType = " + dataType);
            System.out.println("schema.getType() = " + schema.getType());
            System.out.println("schema = " + schema);
            throw e;
//            throw new RuntimeException("", e)
        }
        return (T) o;
    }
}
