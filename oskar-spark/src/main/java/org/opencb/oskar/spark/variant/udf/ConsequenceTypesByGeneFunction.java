package org.opencb.oskar.spark.variant.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.runtime.AbstractFunction2;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.*;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ConsequenceTypesByGeneFunction extends AbstractFunction2<GenericRowWithSchema, String, Collection<String>>
        implements UDF2<GenericRowWithSchema, String, Collection<String>> {

    @Override
    public Collection<String> call(GenericRowWithSchema annotation, String gene) {
        boolean emptyGene = StringUtils.isEmpty(gene);
        Set<String> ct = new HashSet<>();
        List<GenericRowWithSchema> consequenceTypes = annotation.getList(CONSEQUENCE_TYPES_IDX);

        for (GenericRowWithSchema consequenceType : consequenceTypes) {
            if (emptyGene
                    || gene.equals(consequenceType.getString(CONSEQUENCE_TYPES_GENE_NAME_IDX))
                    || gene.equals(consequenceType.getString(CONSEQUENCE_TYPES_ENSEMBL_GENE_ID_IDX))
                    || gene.equals(consequenceType.getString(CONSEQUENCE_TYPES_ENSEMBL_TRANSCRIPT_ID_IDX))) {
                List<GenericRowWithSchema> sequenceOntologyTerms =
                        consequenceType.getList(SEQUENCE_ONTOLOGY_TERM_IDX);
                for (GenericRowWithSchema sequenceOntologyTerm : sequenceOntologyTerms) {
                    ct.add(sequenceOntologyTerm.getString(SEQUENCE_ONTOLOGY_TERM_NAME_IDX));
                }
            }
        }
        return ct;
    }

    @Override
    public Collection<String> apply(GenericRowWithSchema annotation, String gene) {
        return call(annotation, gene);
    }
}
