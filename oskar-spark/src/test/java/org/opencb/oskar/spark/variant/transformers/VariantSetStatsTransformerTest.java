package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;

import java.util.Arrays;
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

        assertEquals(df.count(), stats.getVariantCount().longValue());

        Map<String, Integer> ctMap = new HashMap<>();
        for (Row ct : df.select(explode(consequence_types("annotation")).alias("ct")).groupBy("ct").count().collectAsList()) {
            ctMap.put(ct.getString(0), ((int) ct.getLong(1)));
        }
        assertEquals(ctMap, stats.getConsequenceTypeCount());

        Map<String, Integer> biotypesMap = new HashMap<>();
        for (Row bt : df.select(explode(biotypes("annotation")).alias("bt")).groupBy("bt").count().collectAsList()) {
            if (!bt.getString(0).isEmpty()) {
                biotypesMap.put(bt.getString(0), ((int) bt.getLong(1)));
            }
        }
        assertEquals(biotypesMap, stats.getBiotypeCount());

        System.out.println("stats = " + stats);
    }

    @Test
    public void testVariantSetStatsByFile() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer()
                .setFileId("platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");

        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(df).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        long expectedNumVariants = df.where("file(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz') is not null").count();
        assertEquals(expectedNumVariants, stats.getVariantCount().longValue());

        long expectedNumPass = df.selectExpr("file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'FILTER') as FILTER")
                .filter("FILTER == 'PASS'")
                .count();
//        assertEquals(expectedNumPass, stats.getNumPass().longValue());

        Row row = df.selectExpr("file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') as QUAL")
                .agg(mean("QUAL"), stddev_pop("QUAL"))
                .collectAsList().get(0);
        double expectedMeanQual = row.getDouble(0);
        double expectedStddevQual = row.getDouble(1);
        assertEquals(expectedMeanQual, stats.getQualityAvg().doubleValue(), 0.0001);
        assertEquals(expectedStddevQual, stats.getQualityStdDev().doubleValue(), 0.0001);


        System.out.println("stats = " + stats);
    }

    @Test
    public void testVariantSetStatsBySample() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer()
                .setSamples(Arrays.asList("NA12877", "NA12878"));

        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(df).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        System.out.println("stats = " + stats);

        assertEquals(2, stats.getSampleCount().intValue());

        long expectedNumVariants = df.where("genotype(studies, 'NA12877') RLIKE '1' OR genotype(studies, 'NA12878') RLIKE '1' OR ").count();
        assertEquals(expectedNumVariants, stats.getVariantCount().longValue());
    }
}