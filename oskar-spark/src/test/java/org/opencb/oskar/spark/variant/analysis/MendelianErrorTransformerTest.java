package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

/**
 * Created on 09/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testMendelianErrors() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        new MendelianErrorTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setFather("NA12877").setMother("NA12878").setChild("NA12879")
                .transform(df)
//                .where("code != 0").show();
//                .where(col("code").notEqual(0))
                .show();
    }
}