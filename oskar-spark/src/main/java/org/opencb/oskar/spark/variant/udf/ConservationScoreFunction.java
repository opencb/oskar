package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.*;

/**
 * Read the value for the Conservation Score. Null if none.
 *
 * Main conservation scores are: gerp, phastCons and phylop
 *
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ConservationScoreFunction extends AbstractFunction2<GenericRowWithSchema, String, Double>
        implements UDF2<GenericRowWithSchema, String, Double> {

    @Override
    public Double call(GenericRowWithSchema annotation, String source) {
        WrappedArray<GenericRowWithSchema> conservationScores = annotation.getAs(CONSERVATION_SCORE_IDX);
        for (int i = 0; i < conservationScores.size(); i++) {
            GenericRowWithSchema conservationScore = conservationScores.apply(i);
            if (conservationScore.<String>getAs(SCORE_SOURCE_IDX).equals(source)) {
                return conservationScore.getAs(SCORE_SCORE_IDX);
            }
        }

        return null;
    }

    @Override
    public Double apply(GenericRowWithSchema annotation, String source) {
        return call(annotation, source);
    }
}
