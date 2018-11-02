package org.opencb.oskar.spark.variant.converters;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantStats;
import org.opencb.biodata.tools.Converter;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 12/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToRowConverter implements Converter<Variant, Row>, Serializable {

    public static final StructType VARIANT_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(VariantAvro.getClassSchema()).dataType());
    public static final StructType STATS_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(VariantStats.getClassSchema()).dataType());
    public static final StructType STUDY_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(StudyEntry.getClassSchema()).dataType());
    public static final StructType FILE_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(FileEntry.getClassSchema()).dataType());

    @Override
    public Row convert(Variant variant) {
        return convert(variant.getImpl());
    }

    public Row convert(VariantAvro variantAvro) {
        return (Row) convert(variantAvro, VARIANT_DATA_TYPE);
    }

    public Row convert(StudyEntry studyEntry) {
        return (Row) convert(studyEntry, STUDY_DATA_TYPE);
    }

    public Row convert(VariantStats stats) {
        return (Row) convert(stats, STATS_DATA_TYPE);
    }

    private Object convert(Object o, DataType dataType) {
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
