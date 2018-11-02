package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.AbstractFunction2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IncludeFunction extends AbstractFunction2<GenericRowWithSchema, String, GenericRowWithSchema>
        implements UDF2<GenericRowWithSchema, String, GenericRowWithSchema> {

    @Override
    public GenericRowWithSchema call(GenericRowWithSchema row, String include) {
        Object[] values = new Object[row.length()];

        String[] includeSplit = include.split(",");
        Set<String> fieldsSet = new HashSet<>(includeSplit.length);
        fieldsSet.addAll(Arrays.asList(includeSplit));

        StructType schema = row.schema();
        for (int i = 0; i < row.length(); i++) {
            StructField field = schema.apply(i);
            if (fieldsSet.contains(field.name())) {
                // Include this field
                values[i] = row.get(i);
            } else {
                values[i] = null;
            }
        }

        return new GenericRowWithSchema(values, schema);
    }

    @Override
    public GenericRowWithSchema apply(GenericRowWithSchema row, String include) {
        return call(row, include);
    }
}
