package org.opencb.oskar.spark.variant.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction1;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.CONSEQUENCE_TYPES_GENE_NAME_IDX;
import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.CONSEQUENCE_TYPES_IDX;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenesFunction extends AbstractFunction1<Row, Collection<String>> implements UDF1<Row, Collection<String>> {

    @Override
    public Collection<String> call(Row annotation) {
        Set<String> genes = new HashSet<>();
        List<GenericRowWithSchema> consequenceTypes = annotation.getList(CONSEQUENCE_TYPES_IDX);

        for (GenericRowWithSchema consequenceType : consequenceTypes) {
            String geneName = consequenceType.getString(CONSEQUENCE_TYPES_GENE_NAME_IDX);
            if (StringUtils.isNotEmpty(geneName)) {
                genes.add(geneName);
            }
        }
        return genes;
    }

    @Override
    public Collection<String> apply(Row annotation) {
        return call(annotation);
    }
}
