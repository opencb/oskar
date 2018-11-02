package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PopFreqAsMapFunction
        extends AbstractFunction1<GenericRowWithSchema, Object>
        implements UDF1<GenericRowWithSchema, Object> {

    @Override
    public Object call(GenericRowWithSchema annotation) {
        Map<String, Double> map = new HashMap<>();
        List<GenericRowWithSchema> list = annotation.getList(annotation.fieldIndex("populationFrequencies"));
        if (list != null && list.size() > 0) {
            for (GenericRowWithSchema elem : list) {
                map.put(elem.getAs("study") + ":" + elem.getAs("population"), ((Number) elem.getAs("altAlleleFreq")).doubleValue());
            }
        }
        return map;
    }

    @Override
    public Object apply(GenericRowWithSchema annotation) {
        return call(annotation);
    }
}
