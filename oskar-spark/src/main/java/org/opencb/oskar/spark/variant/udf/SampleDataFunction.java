package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleDataFunction extends AbstractFunction2<Object, String, WrappedArray<String>>
        implements UDF2<Object, String, WrappedArray<String>> {

    @Override
    public WrappedArray<String> call(Object o, String sample) {
        if (o instanceof GenericRowWithSchema) {
            GenericRowWithSchema study = (GenericRowWithSchema) o;
            return getSampleData(study, sample);
        } else if (o instanceof WrappedArray) {
            WrappedArray array = (WrappedArray) o;
            for (int i = 0; i < array.size(); i++) {
                GenericRowWithSchema study = (GenericRowWithSchema) array.apply(i);
                WrappedArray<String> sampleData = getSampleData(study, sample);
                if (sampleData != null) {
                    return sampleData;
                }
            }
        } else {
            throw new IllegalArgumentException("");
        }
        return null;
    }

    static WrappedArray<String> getSampleData(GenericRowWithSchema study, String sample) {
        String studyId = study.getAs("studyId");

        StructType schema = study.schema();
        Metadata metadata = schema.apply("samplesData").metadata();
        Metadata samplesMetadata = metadata.getMetadata("samples");
        String[] sampleNames = samplesMetadata.getStringArray(studyId);
        int i = Arrays.binarySearch(sampleNames, sample);
        if (i >= 0) {
            List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
            return samplesData.get(i);
        } else {
            return null;
        }
    }

    @Override
    public WrappedArray<String> apply(Object o, String sample) {
        return call(o, sample);
    }
}
