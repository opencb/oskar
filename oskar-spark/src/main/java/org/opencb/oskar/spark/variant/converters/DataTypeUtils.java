package org.opencb.oskar.spark.variant.converters;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DataTypeUtils {

    public static StructField addMetadata(Metadata metadata, StructField samplesDataSchema) {
        return new StructField(
                samplesDataSchema.name(),
                samplesDataSchema.dataType(),
                samplesDataSchema.nullable(),
                metadata
        );
    }

    public static StructType replaceField(StructType structType, StructField structField) {
        StructField[] fields = new StructField[structType.size()];
        for (int i = 0; i < structType.size(); i++) {
            StructField field = structType.apply(i);
            if (field.name().equals(structField.name())) {
                fields[i] = structField;
            } else {
                fields[i] = field;
            }
        }
        return new StructType(fields);
    }

}
