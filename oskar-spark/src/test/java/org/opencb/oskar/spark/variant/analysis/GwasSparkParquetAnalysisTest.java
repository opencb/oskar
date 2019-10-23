package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.spark.OskarSparkTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.CHI_SQUARE_TEST;
import static org.opencb.oskar.spark.OskarSparkTestUtils.getRootDir;

public class GwasSparkParquetAnalysisTest {

    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype;
    private ObjectMap executorParams;
    Path outDir;
    private GwasConfiguration configuration;

    private OskarSparkTestUtils oskarSparkTestUtils;

    @Before
    public void init() throws IOException {
        // Init sample lists
        sampleList1 = Arrays.asList("NA12893,NA12880,NA12883,NA12886,NA12890".split(","));
        sampleList2 = Arrays.asList("NA12877,NA12878,NA12879,NA12881,NA12882,NA12884,NA12885,NA12887,NA12888,NA12889,NA12891,NA12892".split(","));

        // Init phenotype
        phenotype = "JJ";

        // Prepare parquet and metadata test files
        oskarSparkTestUtils = new OskarSparkTestUtils();
        File file = oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL);
        oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL + ".meta.json.gz");

        executorParams = new ObjectMap();
        executorParams.put("STUDY_ID", OskarSparkTestUtils.PLATINUM_STUDY);
        executorParams.put("MASTER", "local[*]");
        executorParams.put("FILE", file.getAbsolutePath());

        outDir = oskarSparkTestUtils.getRootDir().toAbsolutePath();

        configuration = new GwasConfiguration();
    }

    private void executeGwasByLists() throws IOException, OskarAnalysisException {
        GwasSparkParquetAnalysis executor = new GwasSparkParquetAnalysis(executorParams, outDir, configuration);
        executor.setSampleList1(sampleList1).setSampleList2(sampleList2).setStudy(OskarSparkTestUtils.PLATINUM_STUDY).setOutputFile(outDir.resolve("gwas.tsv"));

        executor.exec();
    }


    private void executeGwasByPhenotype() throws IOException, OskarAnalysisException {
        GwasSparkParquetAnalysis executor = new GwasSparkParquetAnalysis(executorParams, outDir, configuration);
        executor.setPhenotype1(phenotype).setStudy(OskarSparkTestUtils.PLATINUM_STUDY).setOutputFile(outDir.resolve("gwas.tsv"));
        executor.exec();
    }

    @Test
    public void gwasChiSquareByLists() throws IOException, OskarAnalysisException {
        configuration.setMethod(CHI_SQUARE_TEST);

        executeGwasByLists();

        System.out.println("GWAS/chi square done! Results at " + outDir);
    }

    @Test
    public void gwasChiSquareByPhenotype() throws IOException, OskarAnalysisException {
        configuration.setMethod(CHI_SQUARE_TEST);

        System.out.println("GWAS/chi square done! Results at " + outDir);
    }

    @Test
    public void gwasFisherByLists() throws IOException, OskarAnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);
        configuration.setFisherMode(GwasConfiguration.FisherMode.GREATER);

        executeGwasByLists();

        System.out.println("GWAS/fisher done! Results at " + getRootDir().toAbsolutePath());
    }

    @Test
    public void gwasFisherByPhenotype() throws IOException, OskarAnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);
        configuration.setFisherMode(GwasConfiguration.FisherMode.TWO_SIDED);

        executeGwasByPhenotype();

        System.out.println("GWAS/fisher done! Results at " + getRootDir().toAbsolutePath());
    }

}