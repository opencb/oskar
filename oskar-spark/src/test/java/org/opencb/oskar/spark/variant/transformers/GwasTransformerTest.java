package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.core.exceptions.OskarException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.biotypes;


public class GwasTransformerTest {

    List<String> sampleList1 = Arrays.asList("NA12893,NA12880,NA12883,NA12886,NA12890".split(","));
    List<String> sampleList2 = Arrays.asList("NA12877,NA12878,NA12879,NA12881,NA12882,NA12884,NA12885,NA12887,NA12888,NA12889,NA12891,NA12892".split(","));

    String phenotype1 = "JJ";
    String phenotype2 = "KK";

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void gwasChiSquareTestTwoLists() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setSampleList1(sampleList1)
                .setSampleList2(sampleList2)
                .setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasChiSquareTestOneList() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setSampleList1(sampleList1)
                .setSampleList2(Collections.emptyList())
                .setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasFisherTestTwoLists() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setSampleList1(sampleList1)
                .setSampleList2(sampleList2)
                .setMethod(GwasConfiguration.Method.FISHER_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasFisherTestOneList() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setSampleList1(Collections.emptyList())
                .setSampleList2(sampleList2)
                .setMethod(GwasConfiguration.Method.FISHER_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasChiSquareTestTwoPhenotypes() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype1(phenotype1)
                .setPhenotype2(phenotype2)
                .setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasChiSquareTestOnePhenotype() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype1(phenotype1)
                .setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasFisherTestTwoPhenotypes() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype1(phenotype1)
                .setPhenotype2(phenotype2)
                .setMethod(GwasConfiguration.Method.FISHER_TEST.label)
                .transform(df)
                .show();
    }

    @Test
    public void gwasFisherTestOnePhenotype() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        new GwasTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype2(phenotype2)
                .setMethod(GwasConfiguration.Method.FISHER_TEST.label)
                .transform(df)
                .show();
    }
}