package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.*;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PopulationFrequencyAsMapFunction
        extends AbstractFunction1<GenericRowWithSchema, Object> implements UDF1<GenericRowWithSchema, Object> {

    @Override
    public Object call(GenericRowWithSchema annotation) {
        Map<String, Double> popAltFreq = new HashMap<>();
        List<GenericRowWithSchema> list = annotation.getList(POPULATION_FREQUENCIES_IDX);
        if (list != null && list.size() > 0) {
            for (GenericRowWithSchema elem : list) {
                popAltFreq.put(elem.getAs(POPULATION_FREQUENCIES_STUDY_IDX)
                        + ":" + elem.getAs(POPULATION_FREQUENCIES_POPULATION_IDX),
                        ((Number) elem.getAs(POPULATION_FREQUENCIES_ALT_ALLELE_FREQ_IDX)).doubleValue());
            }
        }
        return popAltFreq;
    }

    @Override
    public Object apply(GenericRowWithSchema annotation) {
        return call(annotation);
    }
}
