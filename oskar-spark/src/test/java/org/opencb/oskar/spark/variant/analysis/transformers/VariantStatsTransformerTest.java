package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.commons.OskarRuntimeException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantStatsTransformer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

/**
 * Created on 05/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testVariantStats() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        long count = df.count();
        long noStats = df.filter(size(col("studies").getItem(0).getField("stats")).equalTo(0)).count();
        assertEquals(count, noStats);

        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort("ALL");
        Dataset<Row> transform = transformer.transform(df);

        long withStats = transform.filter(size(col("studies").getItem(0).getField("stats")).equalTo(1)).count();
        assertEquals(count, withStats);

        transform.select(col("studies").getItem(0).getField("stats").getField("ALL").as("ALL")).selectExpr("ALL.*").show(false);
    }

    @Test
    public void testVariantStatsMissingAsRef() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        long count = df.count();
        long noStats = df.filter(size(col("studies").getItem(0).getField("stats")).equalTo(0)).count();
        assertEquals(count, noStats);

        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort("ALL").setMissingAsReference(true);
        Dataset<Row> transform = transformer.transform(df);

        long withMissing = transform.filter(col("studies").getItem(0).getField("stats").getField("ALL").getField("missingAlleleCount").gt(0)).count();
        assertEquals(0, withMissing);

//        transform.select(col("studies").getItem(0).getField("stats").getField("ALL").as("ALL")).selectExpr("ALL.*").show(false);
    }

    @Test
    public void testCheckMetadata() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        Map<String, List<String>> samplesExpected = sparkTest.getOskar().metadata().samples(df);
        assertEquals(1, samplesExpected.size());
        VariantMetadata origMetadata = sparkTest.getOskar().metadata().variantMetadata(df);
        assertEquals(0, origMetadata.getStudies().get(0).getCohorts().size());

        String cohort1Id = "MY_COHORT";
        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort(cohort1Id);
        df = transformer.transform(df);
        Map<String, List<String>> samples = sparkTest.getOskar().metadata().samples(df);

        assertEquals(samplesExpected, samples);

        VariantMetadata newMetadata = sparkTest.getOskar().metadata().variantMetadata(df);

        assertEquals(1, newMetadata.getStudies().get(0).getCohorts().size());
        assertEquals(cohort1Id, newMetadata.getStudies().get(0).getCohorts().get(0).getId());


        String cohort2Id = "MY_COHORT_2";
        List<String> cohort2Samples = Arrays.asList(NA12877, NA12878, NA12879, NA12880);
        transformer = new VariantStatsTransformer().setSamples(cohort2Samples).setCohort(cohort2Id);
        df = transformer.transform(df);

        newMetadata = sparkTest.getOskar().metadata().variantMetadata(df);

        assertEquals(2, newMetadata.getStudies().get(0).getCohorts().size());
        assertEquals(cohort1Id, newMetadata.getStudies().get(0).getCohorts().get(0).getId());
        assertEquals(cohort2Id, newMetadata.getStudies().get(0).getCohorts().get(1).getId());
        assertEquals(cohort2Samples, newMetadata.getStudies().get(0).getCohorts().get(1).getSampleIds());

        List<Object> keys = df.selectExpr("map_keys(studies[0].stats)").collectAsList().get(0).getList(0);

        assertEquals(2, keys.size());
        assertTrue(keys.contains(cohort1Id));
        assertTrue(keys.contains(cohort2Id));

    }

    @Test
    public void testUnknownSample() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String studyId = "hgvauser@platinum:illumina_platinum";
        List<String> samples = new VariantMetadataManager().samples(df, studyId);
        VariantStatsTransformer transformer = new VariantStatsTransformer().setSamples(Arrays.asList("unknown_sample")).setCohort("ID");
        OskarRuntimeException expected = OskarException.unknownSample(studyId, "unknown_sample", samples);
        thrown.expect(expected.getClass());
        thrown.expectMessage(expected.getMessage());
        transformer.transform(df);
    }


    @Test
    public void testVariantStats00() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        VariantStatsTransformer transformer = new VariantStatsTransformer();
//        transformer.transform(df).select(col("id"), col("studies").getItem(0).getField("stats").getField("ALL").as("cohort"))
//                .selectExpr("id", "cohort.*")


        Dataset<Row> outDf = transformer.transform(df).select(col("id"), col("studies").getItem(0).getField("stats").getField("ALL").as("cohort"))
                .selectExpr("id", "cohort.*");

        StringBuilder line = new StringBuilder("#");
        for (StructField field : outDf.schema().fields()) {
            if (line.length() != 1) {
                line.append("\t");
            }
            line.append(field.name());
        }

        System.out.println(line);

        Iterator<Row> rowIterator = outDf.toLocalIterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            line.setLength(0);
            for (int i = 0; i < row.size(); i++) {
                if (line.length() != 0) {
                    line.append("\t");
                }
                if (row.get(i) instanceof scala.collection.immutable.Map) {
                    scala.collection.Map<Object, Object> map = row.getMap(i);
                    scala.collection.Iterator<Object> iterator = map.keys().iterator();
                    while (iterator.hasNext()) {
                        Object key = iterator.next();
                        line.append(key).append(":").append(map.get(key).get()).append(";");
                    }
                } else {
                    line.append(row.get(i));
                }
            }
            System.out.println(line);
            break;
        }

//                .show(false);
//        System.out.println("count = " + count);
        //.getItem(0).getField("stats").getField("ALL").as("ALL")).selectExpr("ALL.*").show(false);



//        long count = df.count();
//        long noStats = df.filter(size(col("studies").getItem(0).getField("stats")).equalTo(0)).count();
//        assertEquals(count, noStats);
//
//        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort("ALL");
//        Dataset<Row> transform = transformer.transform(df);
//
//        long withStats = transform.filter(size(col("studies").getItem(0).getField("stats")).equalTo(1)).count();
//        assertEquals(count, withStats);
//
//        transform.select(col("studies").getItem(0).getField("stats").getField("ALL").as("ALL")).selectExpr("ALL.*").show(false);
    }
}