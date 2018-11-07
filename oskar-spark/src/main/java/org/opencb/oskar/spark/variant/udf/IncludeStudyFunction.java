package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.STUDY_ID_IDX;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IncludeStudyFunction
        extends AbstractFunction2<WrappedArray<GenericRowWithSchema>, String, WrappedArray<GenericRowWithSchema>>
        implements UDF2<WrappedArray<GenericRowWithSchema>, String, WrappedArray<GenericRowWithSchema>> {

    @Override
    public WrappedArray<GenericRowWithSchema> call(WrappedArray<GenericRowWithSchema> studies, String studiesToInclude) {
        HashSet<String> set = new HashSet<>(Arrays.asList(studiesToInclude.split(",")));
        List<GenericRowWithSchema> filteredStudies = new ArrayList<>(studies.length());

        for (int i = 0; i < studies.length(); i++) {
            GenericRowWithSchema study = studies.apply(i);
            if (set.contains(study.getString(STUDY_ID_IDX))) {
                filteredStudies.add(study);
            }
        }

        return WrappedArray.<GenericRowWithSchema>make(filteredStudies.toArray(new GenericRowWithSchema[filteredStudies.size()]));
    }

    @Override
    public WrappedArray<GenericRowWithSchema> apply(WrappedArray<GenericRowWithSchema> studies, String include) {
        return call(studies, include);
    }
}
