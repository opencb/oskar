package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.converters.DataframeToFacetFieldConverter;

import java.io.IOException;

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
}