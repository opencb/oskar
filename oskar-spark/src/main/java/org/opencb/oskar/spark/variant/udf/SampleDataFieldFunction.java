package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction3;

import java.util.List;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleDataFieldFunction extends AbstractFunction3<Object, String, String, String>
        implements UDF3<Object, String, String, String> {

    @Override
    public String call(Object o, String sample, String formatField) {
        if (o instanceof GenericRowWithSchema) {
            GenericRowWithSchema study = (GenericRowWithSchema) o;
            return getSampleDataField(study, sample, formatField);
        } else if (o instanceof WrappedArray) {
            WrappedArray array = (WrappedArray) o;
            for (int i = 0; i < array.size(); i++) {
                GenericRowWithSchema study = (GenericRowWithSchema) array.apply(i);
                String sampleDataField = getSampleDataField(study, sample, formatField);
                if (sampleDataField != null) {
                    return sampleDataField;
                }
            }
        } else {
            throw new IllegalArgumentException("");
        }
        return null;
    }

    private String getSampleDataField(GenericRowWithSchema study, String sample, String formatField) {
        List<String> format = study.getList(study.fieldIndex("format"));
        int idx = format.indexOf(formatField);
        if (idx < 0) {
            return null;
        }

        WrappedArray<String> sampleData = SampleDataFunction.getSampleData(study, sample);
        if (sampleData == null) {
            return null;
        }
        return sampleData.apply(idx);
    }

    @Override
    public String apply(Object o, String sample, String formatField) {
        return call(o, sample, formatField);
    }
}
