package org.opencb.oskar.spark.variant.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.size;
import static org.junit.Assert.*;

/**
 * Created on 05/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testVariantStatsML() throws Exception {

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
}