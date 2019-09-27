package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.GwasSparkParquetAnalysisExecutor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GwasSparkParquetAnalysisExecutorTest {

    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype;
    private ObjectMap executorParams;
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

        configuration = new GwasConfiguration();
    }

    private AnalysisResult executeGwasByLists() throws IOException, AnalysisException {
        GwasSparkParquetAnalysisExecutor executor = new GwasSparkParquetAnalysisExecutor(sampleList1, sampleList2, executorParams,
                oskarSparkTestUtils.getRootDir().toAbsolutePath(), configuration);
        return executor.exec();
    }

    private AnalysisResult executeGwasByPhenotype() throws IOException, AnalysisException {
        GwasSparkParquetAnalysisExecutor executor = new GwasSparkParquetAnalysisExecutor(phenotype, executorParams,
                oskarSparkTestUtils.getRootDir().toAbsolutePath(), configuration);
        return executor.exec();
    }

    @Test
    public void gwasChiSquareByLists() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST);

        AnalysisResult analysisResult = executeGwasByLists();

        System.out.println("GWAS/chi square done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasChiSquareByPhenotype() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.CHI_SQUARE_TEST);

        AnalysisResult analysisResult = executeGwasByPhenotype();

        System.out.println("GWAS/chi square done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasFisherByLists() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);

        AnalysisResult analysisResult = executeGwasByLists();

        System.out.println("GWAS/fisher done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasFisherByPhenotype() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);

        AnalysisResult analysisResult = executeGwasByPhenotype();

        System.out.println("GWAS/fisher done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }
}