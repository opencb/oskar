package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.VariantClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.core.exceptions.OskarException;
import org.opencb.oskar.spark.variant.converters.DataframeToFacetFieldConverter;
import org.opencb.oskar.spark.variant.converters.RowToVariantConverter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.*;

public class FacetTransformerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void emptyFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        FacetTransformer facetTransformer = new FacetTransformer();

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();
    }

    @Test
    public void simpleFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "type";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();

        FacetQueryResult.Field field = new DataframeToFacetFieldConverter().convert(res);
    }

    @Test
    public void simpleAndIncludeFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "biotype[protein_coding,miRNA,retained_intron]";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();
    }

    @Test
    public void rangeFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "gerp[-3..3]:1";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();
    }

    @Test
    public void aggregationFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "avg(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();
    }

    @Test
    public void uniqueAggFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        df.withColumn("gene", explode(genes("annotation")))
                .withColumn("biotype", explode(biotypes("annotation")))
//                .groupBy("gene").agg(expr("collect_list(start)"), expr("sum(power(start, 2))"))
//              .groupBy("gene").agg(expr("percentile(start, array(0.25, 0.5, 0.75))"))
//              .groupBy("gene").agg(collect_list("biotype"))
                .show(false);
    }

    @Test
    public void nestedFacetCatAndCat() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "biotype>>type";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show();

        new DataframeToFacetFieldConverter().convert(res);
    }

    @Test
    public void nestedFacetCatAndCatAndRange() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "biotype>>type>>gerp[-10..10]:0.5";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);
    }

    @Test
    public void nestedFacetCatAndCatAndRangeAndRange() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "cadd_raw[-100..100]:10>>biotype>>type>>gerp[-10..10]:0.5>>gene[CNN2P1,EIF4ENIF1,IGLV3-12,CTA-85E5.10]";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);
    }

    @Test
    public void nestedFacetCatAndAggAndCat() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "biotype>>avg(gerp)>>type";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);
    }

    @Test
    public void nestedFacetCatAndCatAndAgg() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "biotype>>type>>avg(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void aggSumSq() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "sumsq(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void aggPercentile() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "percentile(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void aggAvg() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "biotype>>gene>>avg(start)";
//        String facet = "avg(start)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void nestedFacetCatAndCatAndPercentileSq() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "biotype>>type>>percentile(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void nestedFacetCatAndCatAndPercentile() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "biotype>>type>>percentile(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100, false);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void aggFacet() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "avg(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void nestedFacetCatAndRangeAndAgg() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        String facet = "gene>>biotype>>cadd_raw[-100..100]:20>>cadd_scaled[-100..100]:20>>avg(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);

        Dataset<Row> res = facetTransformer.transform(df);
        res.show(100);

        System.out.println(res.schema().apply("count").metadata().getString("facet"));

        System.out.println(new DataframeToFacetFieldConverter().convert(res).toString());
    }

    @Test
    public void nestedFacetCatAndPopFreqRange() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        // popFred__xxx__yyy where xxx = study, yyy = population
        String facet = "biotype>>popFreq__GNOMAD_GENOMES__AMR[0..1]:0.1";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void popFreqPercentile() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "percentile(popFreq__GNOMAD_GENOMES__ALL)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void ct() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "ct";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void typeAndCtAndAvgGerp() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "type>>ct>>avg(gerp)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void substitutionScore() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "polyphen";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(100,false);
    }

    @Test
    public void substitutionScoreRange() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String facet = "biotype>>polyphen[0..1]:0.1";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(100,false);
    }

    @Test
    public void stats() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String studyId = "hgvauser@platinum:illumina_platinum";
        String cohort = "ALL";

        List<String> sampleNames = sparkTest.getOskar().metadata().samples(df).get(studyId);

        df = sparkTest.getOskar().stats(df, studyId, cohort, sampleNames);

        String facet = "stats__hgvauser@platinum:illumina_platinum__ALL";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void statsRange() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String studyId = "hgvauser@platinum:illumina_platinum";
        String cohort = "ALL";

        List<String> sampleNames = sparkTest.getOskar().metadata().samples(df).get(studyId);

        df = sparkTest.getOskar().stats(df, studyId, cohort, sampleNames);

        String facet = "biotype>>stats__hgvauser@platinum:illumina_platinum__ALL[0..1]:0.1";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }

    @Test
    public void statsAgg() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String studyId = "hgvauser@platinum:illumina_platinum";
        String cohort = "ALL";

        List<String> sampleNames = sparkTest.getOskar().metadata().samples(df).get(studyId);

        df = sparkTest.getOskar().stats(df, studyId, cohort, sampleNames);

        String facet = "biotype>>avg(stats__hgvauser@platinum:illumina_platinum__ALL)";
        FacetTransformer facetTransformer = new FacetTransformer();
        facetTransformer.setFacet(facet);
        facetTransformer.transform(df).show(false);
    }


    public void savingAnnotatedVariants() throws IOException, OskarException {
        String assembly = "GRCh38"; // "GRCh37", "GRCh38"
        // CellBase client
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections.singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        CellBaseClient cellBaseClient = new CellBaseClient("hsapiens", assembly, clientConfiguration);

        Dataset<Row> df = sparkTest.getVariantsDataset();
        List<Row> rows = df.collectAsList();
        List<Variant> variants = new ArrayList<>();
        RowToVariantConverter converter = new RowToVariantConverter();
        for (Row row : rows) {
            Variant variant = converter.convert(row);
            variants.add(variant);
        }

        VariantClient variantClient = cellBaseClient.getVariantClient();
        QueryResponse<Variant> annotatedVariants = variantClient.annotate(variants, QueryOptions.empty());

        PrintWriter pw = new PrintWriter("/tmp/variants.json");
        for (Variant variant : annotatedVariants.allResults()) {
            pw.write(variant.toJson() + "\n");
        }
        pw.close();
    }
}