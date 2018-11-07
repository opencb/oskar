package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction3;

import java.util.List;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.*;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PopulationFrequencyFunction extends AbstractFunction3<GenericRowWithSchema, String, String, Object>
        implements UDF3<GenericRowWithSchema, String, String, Object> {

    @Override
    public Object call(GenericRowWithSchema annotation, String study, String population) {
        double freq = 0;
        List<GenericRowWithSchema> list = annotation.getList(POPULATION_FREQUENCIES_IDX);
        if (list != null && list.size() > 0) {
            for (GenericRowWithSchema elem : list) {
                if (study.equals(elem.getAs(POPULATION_FREQUENCIES_STUDY_IDX))
                        && population.equals(elem.getAs(POPULATION_FREQUENCIES_POPULATION_IDX))) {
                    freq = ((Number) elem.getAs(POPULATION_FREQUENCIES_ALT_ALLELE_FREQ_IDX)).doubleValue();
                    break;
                }
            }
        }
        return freq;
    }

    @Override
    public Object apply(GenericRowWithSchema annotation, String study, String population) {
        return call(annotation, study, population);
    }

}
