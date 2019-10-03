package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.result.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.AnalysisResultManager;
import org.opencb.oskar.analysis.variant.gwas.Gwas;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.GwasSparkParquetAnalysisExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.CHI_SQUARE_TEST;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.FISHER_TEST;
import static org.opencb.oskar.spark.OskarSparkTestUtils.getRootDir;

public class GwasSparkParquetAnalysisExecutorTest {

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

    private AnalysisResult executeGwasByLists() throws IOException, AnalysisException {
        GwasSparkParquetAnalysisExecutor executor = new GwasSparkParquetAnalysisExecutor(executorParams, outDir, configuration);
        executor.setSampleList1(sampleList1).setSampleList2(sampleList2);

        AnalysisResultManager amr = new AnalysisResultManager(getRootDir()).init(Gwas.ID, new ObjectMap());
        executor.init(amr);
        executor.exec();

        return amr.close();
    }


    private AnalysisResult executeGwasByPhenotype() throws IOException, AnalysisException {
        GwasSparkParquetAnalysisExecutor executor = new GwasSparkParquetAnalysisExecutor(executorParams, outDir, configuration);
        executor.setPhenotype1(phenotype);
        AnalysisResultManager amr = new AnalysisResultManager(getRootDir()).init("", new ObjectMap());
        executor.init(amr);
        executor.exec();

        return amr.close();
    }

    @Test
    public void gwasChiSquareByLists() throws IOException, AnalysisException {
        configuration.setMethod(CHI_SQUARE_TEST);

        AnalysisResult analysisResult = executeGwasByLists();

        System.out.println("GWAS/chi square done! Results at " + outDir);
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasChiSquareByPhenotype() throws IOException, AnalysisException {
        configuration.setMethod(CHI_SQUARE_TEST);

        AnalysisResult analysisResult = executeGwasByPhenotype();

        System.out.println("GWAS/chi square done! Results at " + outDir);
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasFisherByLists() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);
        configuration.setFisherMode(GwasConfiguration.FisherMode.GREATER);

        AnalysisResult analysisResult = executeGwasByLists();

        System.out.println("GWAS/fisher done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasFisherByPhenotype() throws IOException, AnalysisException {
        configuration.setMethod(GwasConfiguration.Method.FISHER_TEST);
        configuration.setFisherMode(GwasConfiguration.FisherMode.TWO_SIDED);

        AnalysisResult analysisResult = executeGwasByPhenotype();

        System.out.println("GWAS/fisher done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }

    @Test
    public void gwasAnalysisByLists() throws AnalysisException {
        executorParams.put("ID", "spark-parquet");
        configuration.setMethod(FISHER_TEST);
        configuration.setFisherMode(GwasConfiguration.FisherMode.TWO_SIDED);

        Gwas gwas = new Gwas(executorParams, outDir, configuration);
        gwas.setSampleList1(sampleList1).setSampleList2(sampleList2);

        AnalysisResult result = gwas.execute();
        System.out.println(result.toString());

        System.out.println("Results at: " + outDir);
    }

    @Test
    public void gwasAnalysisByPhenotype() throws AnalysisException {
        executorParams.put("ID", "spark-parquet");
        configuration.setMethod(CHI_SQUARE_TEST);

        Gwas gwas = new Gwas(executorParams, outDir, configuration);
        gwas.setPhenotype1("JJ");

        AnalysisResult result = gwas.execute();
        System.out.println(result.toString());

        System.out.println("Results at: " + outDir);
    }
}