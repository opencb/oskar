package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.*;

/**
 * Created on 20/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HistogramTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testHistogramStart() throws IOException, OskarException {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        df = new HistogramTransformer().transform(df);
//        df = new HistogramTransformer().setStep(100000).setInputCol("start").transform(df);
        df.show();

        assertEquals(DataTypes.IntegerType, df.schema().apply("start").dataType());
        assertEquals(DataTypes.LongType, df.schema().apply("count").dataType());
    }

    @Test
    public void testHistogramConservation() throws IOException, OskarException {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        df = df.select(population_frequency("annotation", "GNOMAD_GENOMES", "ALL").as("pf"));
        df = new HistogramTransformer().setStep(0.1).setInputCol("pf").transform(df);
        df.show();

        assertEquals(DataTypes.DoubleType, df.schema().apply("pf").dataType());
        assertEquals(DataTypes.LongType, df.schema().apply("count").dataType());
    }

    @Test
    public void testHistogramWrongColumn() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Input column must be NumericalType");
        new HistogramTransformer().setStep(100000).setInputCol("annotation").transform(df).show();
    }
}