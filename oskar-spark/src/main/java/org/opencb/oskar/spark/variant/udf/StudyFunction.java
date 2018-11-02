package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyFunction extends AbstractFunction2<WrappedArray<? extends Row>, String, Row>
        implements UDF2<WrappedArray<? extends Row>, String, Row> {

    @Override
    public Row call(WrappedArray<? extends Row> studies, String studyId) {
        for (int i = 0; i < studies.length(); i++) {
            Row study = studies.apply(i);
            if (studyId.equals(study.getString(study.fieldIndex("studyId")))) {
                return study;
            }
        }
        return null;
    }

    @Override
    public Row apply(WrappedArray<? extends Row> studies, String studyId) {
        return call(studies, studyId);
    }
}
