package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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