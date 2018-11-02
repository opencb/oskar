package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction1;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ConsequenceTypesFunction
        extends AbstractFunction1<GenericRowWithSchema, Collection<String>>
        implements UDF1<GenericRowWithSchema, Collection<String>> {

    @Override
    public Collection<String> call(GenericRowWithSchema annotation) {
        Set<String> ct = new HashSet<>();
        List<GenericRowWithSchema> consequenceTypes = annotation.getList(annotation.fieldIndex("consequenceTypes"));

        for (GenericRowWithSchema consequenceType : consequenceTypes) {
            List<GenericRowWithSchema> sequenceOntologyTerms = consequenceType.getList(consequenceType.fieldIndex("sequenceOntologyTerms"));
            for (GenericRowWithSchema sequenceOntologyTerm : sequenceOntologyTerms) {
                ct.add(sequenceOntologyTerm.getAs("name"));
            }
        }
        return ct;
    }

    @Override
    public Collection<String> apply(GenericRowWithSchema annotation) {
        return call(annotation);
    }
}
