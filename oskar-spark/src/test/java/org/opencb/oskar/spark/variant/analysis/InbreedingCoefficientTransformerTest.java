package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;

/**
 * Created on 12/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class InbreedingCoefficientTransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();
    private static Dataset<Row> inputDf;

    @BeforeClass
    public static void setUp() throws Exception {
        inputDf = sparkTest.getVariantsDataset();
        inputDf = new VariantStatsTransformer().transform(inputDf).cache();
    }

    @Test
    public void includeMissingGt() throws IOException, OskarException {
        InbreedingCoefficientTransformer t = new InbreedingCoefficientTransformer();
        t.setMissingGenotypesAsHomRef(true);

        Dataset<Row> df = t.transform(inputDf);

        df.printSchema();

        df.show();
    }

    @Test
    public void excludeMissingGt() throws IOException, OskarException {
        InbreedingCoefficientTransformer t = new InbreedingCoefficientTransformer();
        t.setMissingGenotypesAsHomRef(false);

        Dataset<Row> df  = t.transform(inputDf);

        df.printSchema();

        df.show();
    }

    @Test
    public void mafThreshold() throws IOException, OskarException {
        InbreedingCoefficientTransformer t = new InbreedingCoefficientTransformer();
        t.setMafThreshold(0.1);

        Dataset<Row> df  = t.transform(inputDf);

        df.printSchema();

        df.show();
    }

}