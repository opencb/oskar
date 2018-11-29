package org.opencb.oskar.spark.variant.udf;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.CONSEQUENCE_TYPES_ENSEMBL_GENE_ID_IDX;

/**
 * Created on 27/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class EnsemblGenesFunction extends AbstractGenesFunction {

    public EnsemblGenesFunction() {
        super(CONSEQUENCE_TYPES_ENSEMBL_GENE_ID_IDX);
    }
}
