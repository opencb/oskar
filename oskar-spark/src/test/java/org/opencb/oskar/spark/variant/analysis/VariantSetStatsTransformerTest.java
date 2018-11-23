package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.*;

/**
 * Created on 05/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSetStatsTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testVariantSetStats() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer();
        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(df).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        assertEquals(df.count(), stats.getNumVariants().longValue());

        Map<String, Integer> ctMap = new HashMap<>();
        for (Row ct : df.select(explode(consequence_types("annotation")).alias("ct")).groupBy("ct").count().collectAsList()) {
            ctMap.put(ct.getString(0), ((int) ct.getLong(1)));
        }
        assertEquals(ctMap, stats.getConsequenceTypesCounts());

        Map<String, Integer> biotypesMap = new HashMap<>();
        for (Row bt : df.select(explode(biotypes("annotation")).alias("bt")).groupBy("bt").count().collectAsList()) {
            if (!bt.getString(0).isEmpty()) {
                biotypesMap.put(bt.getString(0), ((int) bt.getLong(1)));
            }
        }
        assertEquals(biotypesMap, stats.getVariantBiotypeCounts());

        System.out.println("stats = " + stats);
    }

    @Test
    public void testVariantSetStatsByFile() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer()
                .setFileId("platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");

        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(df).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        long expectedNumPass = df.selectExpr("file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'FILTER') as FILTER")
                .filter("FILTER == 'PASS'")
                .count();
        assertEquals(expectedNumPass, stats.getNumPass().longValue());

        Row row = df.selectExpr("file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') as QUAL")
                .agg(mean("QUAL"), stddev_pop("QUAL"))
                .collectAsList().get(0);
        double expectedMeanQual = row.getDouble(0);
        double expectedStddevQual = row.getDouble(1);
        assertEquals(expectedMeanQual, stats.getMeanQuality().doubleValue(), 0.0001);
        assertEquals(expectedStddevQual, stats.getStdDevQuality().doubleValue(), 0.0001);


        System.out.println("stats = " + stats);
    }
}