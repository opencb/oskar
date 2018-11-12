package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class FisherTransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testFisher() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        new FisherTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .transform(df)
//                .where("code != 0").show();
//                .where(col("code").notEqual(0))
                .show();
    }
}