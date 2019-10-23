package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.core.exceptions.OskarException;

import java.io.IOException;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.genes;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.sample_data_field;

/**
 * Created on 07/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozigoteTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void test() throws IOException, OskarException {
        CompoundHeterozigoteTransformer transformer = new CompoundHeterozigoteTransformer()
                .setFather("NA12877")
                .setMother("NA12878")
                .setChild("NA12879")
                .setMissingGenotypeAsReference(true);

        Dataset<Row> df = sparkTest.getVariantsDataset();
        Dataset<Row> compound = transformer.transform(df);
        compound.cache().show();

        df.join(compound, df.apply("id").equalTo(compound.apply("variant")))
                .select(
                        col("id"),
                        sample_data_field("studies", "NA12877", "GT").as("Father"),
                        sample_data_field("studies", "NA12878", "GT").as("Mother"),
                        sample_data_field("studies", "NA12879", "GT").as("Child"),
                        genes("annotation")
                ).show();

        df = df.where(array_contains(genes("annotation"), "DRG1"));
        transformer.transform(df).show();
    }
}