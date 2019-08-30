package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.analysis.transformers.PCATransformer;

import java.io.IOException;

public class PCATransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void pca() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String studyId = "hgvauser@platinum:illumina_platinum";
        int k = 2;

        new PCATransformer().setStudyId(studyId)
                .setK(k)
                .transform(df.limit(5))
                .show(false);
    }
}