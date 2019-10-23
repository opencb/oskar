package org.opencb.oskar.spark.variant.transformers.params;

import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.Params;

/**
 * Created on 23/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface HasStudyId extends Params {

    default Param<String> studyIdParam() {
        return new Param<>(this, "studyId", "Id of the study to be used.");
    }

    default String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    default HasStudyId setStudyId(String study) {
        set(studyIdParam(), study);
        return this;
    }

}
