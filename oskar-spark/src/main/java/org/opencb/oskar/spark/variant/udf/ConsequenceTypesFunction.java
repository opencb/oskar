package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction1;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.*;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ConsequenceTypesFunction extends AbstractFunction1<GenericRowWithSchema, Collection<String>>
        implements UDF1<GenericRowWithSchema, Collection<String>> {

    @Override
    public Collection<String> call(GenericRowWithSchema annotation) {
        Set<String> consequenceTypeNames = new HashSet<>();
        List<GenericRowWithSchema> consequenceTypes = annotation.getList(CONSEQUENCE_TYPES_IDX);

        for (GenericRowWithSchema consequenceType : consequenceTypes) {
            List<GenericRowWithSchema> sequenceOntologyTerms = consequenceType.getList(SEQUENCE_ONTOLOGY_TERM_IDX);
            for (GenericRowWithSchema sequenceOntologyTerm : sequenceOntologyTerms) {
                consequenceTypeNames.add(sequenceOntologyTerm.getString(SEQUENCE_ONTOLOGY_TERM_NAME_IDX));
            }
        }
        return consequenceTypeNames;
    }

    @Override
    public Collection<String> apply(GenericRowWithSchema annotation) {
        return call(annotation);
    }
}
