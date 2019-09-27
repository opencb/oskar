package org.opencb.oskar.spark.variant.analysis.transformers;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.oskar.spark.OskarSparkTestUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

/**
 * Created on 12/09/19.
 *
 * @author Joaquin Tarraga &lt;joaquintarraga@gmail.com&gt;
 */
public class VariantSampleStatsTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testVariantSampleStats() throws Exception {

        Dataset<Row> inputDs = sparkTest.getVariantsDataset();

        inputDs.printSchema();

        VariantSampleStatsTransformer transformer = new VariantSampleStatsTransformer();
        Dataset<Row> outputDs = transformer.setSamples(NA12877, NA12879, NA12885, NA12890).transform(inputDs);

        outputDs.show(false);
    }

    public void changeParquetSchema() throws IOException {
        String newFilename = "/tmp/new.parquet";

        Path oldPath = sparkTest.getFile(PLATINUM_SMALL).toPath();

        sparkTest.getSpark()
                .read()
                .schema(((StructType) SchemaConverters.toSqlType(VariantAvro.getClassSchema()).dataType()))
                .format("parquet")
                .load(oldPath.toString())
                .write()
                .parquet(newFilename);
    }
}