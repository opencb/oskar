package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF2;
import scala.runtime.AbstractFunction2;

/**
 * Created on 27/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class GenotypeFunction  extends AbstractFunction2<Object, String, String>
        implements UDF2<Object, String, String> {

    private final SampleDataFieldFunction sampleDataFieldFunction = new SampleDataFieldFunction();

    @Override
    public String call(Object o, String sample) {
        return sampleDataFieldFunction.call(o, sample, "GT");
    }


    @Override
    public String apply(Object o, String sample) {
        return call(o, sample);
    }
}
