package org.opencb.oskar.spark.variant.udf;

import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.CONSEQUENCE_TYPES_GENE_NAME_IDX;

/**
 * Created on 27/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenesFunction extends AbstractGenesFunction {

    public GenesFunction() {
        super(CONSEQUENCE_TYPES_GENE_NAME_IDX);
    }
}
