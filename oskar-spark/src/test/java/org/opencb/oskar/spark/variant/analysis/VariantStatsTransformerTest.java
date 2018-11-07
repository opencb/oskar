package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import static org.junit.Assert.assertEquals;

/**
 * Created on 05/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testVariantStats() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        long count = df.count();
        long noStats = df.filter(size(col("studies").getItem(0).getField("stats")).equalTo(0)).count();
        assertEquals(count, noStats);

        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort("ALL");
        Dataset<Row> transform = transformer.transform(df);
        transform.show();

        long withStats = transform.filter(size(col("studies").getItem(0).getField("stats")).equalTo(1)).count();
        assertEquals(count, withStats);

        transform.select(col("studies").getItem(0).getField("stats").getField("ALL").as("ALL")).selectExpr("ALL.*").show(false);
//        df.select(col("studies").getItem(0).getField("samplesData"), col("studies").getItem(0).getField("stats")).show(false);
    }

    @Test
    public void testPreserveMetadata() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        Map<String, List<String>> samplesExpected = sparkTest.getOskar().samples(df);
        assertEquals(1, samplesExpected.size());

        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort("ALL");
        Dataset<Row> transform = transformer.transform(df);
        Map<String, List<String>> samples = sparkTest.getOskar().samples(transform);

        assertEquals(samplesExpected, samples);
    }
}