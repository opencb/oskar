package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.IBDExpectedFrequencies;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.converters.RowToVariantConverter;
import org.opencb.oskar.spark.variant.udf.SampleDataFunction;
import scala.Function1;

import java.util.List;

import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

/**
 * Created on 20/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IBSTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void test() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();

//        new IBSTransformer().setSamples(NA12877, NA12878, NA12879, NA12880).transform(df).show();
//        new IBSTransformer().setSamples(NA12877, NA12878).transform(df).show();
        Dataset<Row> df0 = new IBSTransformer().setNumPairs(10).transform(df);
        df0.show(false);
        long values = df0.count();

        int n = 17;
        Assert.assertEquals(n * (n - 1) / 2, values);

    }
}