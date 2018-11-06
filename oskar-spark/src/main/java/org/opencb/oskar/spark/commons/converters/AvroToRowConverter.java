package org.opencb.oskar.spark.commons.converters;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.WrappedArray;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 06/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AvroToRowConverter {

    protected Object convert(Object o, DataType dataType) {
        if (o instanceof Map) {
            Map<Object, Object> objectMap = new HashMap<>();
            for (Map.Entry entry : ((Map<?, ?>) o).entrySet()) {
                Object value = convert(entry.getValue(), ((MapType) dataType).valueType());
                objectMap.put(entry.getKey().toString(), value);
            }
            o = objectMap;
        } else if (o instanceof Enum) {
            o = o.toString();
        } else if (o instanceof Collection) {
            Object[] objects = new Object[((Collection) o).size()];
            int i = 0;
            for (Object element : ((Collection) o)) {
                objects[i] = convert(element, ((ArrayType) dataType).elementType());
                i++;
            }
            o = WrappedArray.make(objects);
        } else if (o instanceof GenericRecord) {
            Object[] objects = new Object[((StructType) dataType).fields().length];
            int i = 0;
            for (StructField structField : ((StructType) dataType).fields()) {
                Object o1 = ((GenericRecord) o).get(structField.name());
                o1 = convert(o1, structField.dataType());
                objects[i] = o1;
                i++;
            }
            o = new GenericRowWithSchema(objects, (StructType) dataType);
        }
        return o;
    }

}
