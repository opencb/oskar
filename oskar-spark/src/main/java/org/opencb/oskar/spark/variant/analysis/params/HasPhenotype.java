package org.opencb.oskar.spark.variant.analysis.params;

import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.Params;

/**
 * Created on 23/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface HasPhenotype extends Params {

    default Param<String> phenotypeParam() {
        return new Param<>(this, "phenotype", "Specify the phenotype to use for the mode  of inheritance");
    }

    default HasPhenotype setPhenotype(String phenotype) {
        set(phenotypeParam(), phenotype);
        return this;
    }

    default String getPhenotype() {
        return getOrDefault(phenotypeParam());
    }

}
