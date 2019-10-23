package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.core.exceptions.OskarException;

import java.io.IOException;

public class ChromDensityTransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void chromDensity() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        ChromDensityTransformer chromDensityTransformer = new ChromDensityTransformer();
        chromDensityTransformer.setStep(1000000);
        chromDensityTransformer.setChroms("21,22");

        chromDensityTransformer.transform(df).show(100, false);
    }

}