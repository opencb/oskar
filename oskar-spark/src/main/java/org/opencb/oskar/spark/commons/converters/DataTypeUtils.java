package org.opencb.oskar.spark.commons.converters;

import org.apache.spark.sql.types.*;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DataTypeUtils {

    public static StructField addMetadata(Metadata metadata, StructField structField) {
        return new StructField(
                structField.name(),
                structField.dataType(),
                structField.nullable(),
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
        return DataTypes.createStructType(fields);
    }

    public static StructType addField(StructType structType, StructField structField) {
        StructField[] fields = new StructField[structType.size() + 1];
        for (int i = 0; i < structType.size(); i++) {
            fields[i] = structType.apply(i);
        }
        fields[fields.length - 1] = structField;
        return DataTypes.createStructType(fields);
    }

    public static int getFieldIdx(StructType schema, String path) {
        StructType rootSchema = schema;
        DataType dataType;
        String[] split = path.split("\\.");
        for (int i = 0; i < split.length - 1; i++) {
            String name = split[i];
            dataType = schema.apply(name).dataType();
            while (dataType instanceof ArrayType) {
                dataType = ((ArrayType) dataType).elementType();
            }
            if (dataType instanceof StructType) {
                schema = ((StructType) dataType);
            } else {
                throw new IllegalStateException("Path " + path + " not found in " + rootSchema);
            }
        }
        return schema.fieldIndex(split[split.length - 1]);
    }
}
