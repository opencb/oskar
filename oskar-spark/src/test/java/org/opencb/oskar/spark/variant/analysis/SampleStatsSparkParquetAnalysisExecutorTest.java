package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.AnalysisResult;
import org.opencb.oskar.analysis.result.AnalysisResultManager;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.SampleVariantStatsSparkParquetAnalysisExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

public class SampleStatsSparkParquetAnalysisExecutorTest {
    private List<String> sampleNames;
    private String cohort;
    private ObjectMap executorParams;

    private Path outDir;

    private OskarSparkTestUtils oskarSparkTestUtils;


    @Before
    public void init() throws IOException {
        // Prepare parquet and metadata test files
        oskarSparkTestUtils = new OskarSparkTestUtils();
        File file = oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL);
        oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL + ".meta.json.gz");

        outDir = oskarSparkTestUtils.getRootDir().toAbsolutePath();

        executorParams = new ObjectMap();
        executorParams.put("STUDY_ID", OskarSparkTestUtils.PLATINUM_STUDY);
        executorParams.put("MASTER", "local[*]");
        executorParams.put("FILE", file.getAbsolutePath());

        sampleNames = new ArrayList<>();
        sampleNames.add(NA12877);
        sampleNames.add(NA12879);
        sampleNames.add(NA12885);
        sampleNames.add(NA12890);
    }

    @Test
    public void sampleStatsExecutor() throws IOException, AnalysisException {
        SampleVariantStatsSparkParquetAnalysisExecutor executor = new SampleVariantStatsSparkParquetAnalysisExecutor(executorParams, outDir);
        executor.setSampleNames(sampleNames);

        AnalysisResultManager amr = new AnalysisResultManager(getRootDir()).init("", new ObjectMap());
        executor.init(amr);
        executor.exec();
        AnalysisResult analysisResult = amr.close();

        System.out.println("Sample stats done! Results at " + outDir);
        System.out.println(analysisResult.toString());
    }

    @Test
    public void sampleStatsAnalysisBySampleList() throws AnalysisException {
        executorParams.put("ID", "spark-parquet");

        SampleVariantStatsAnalysis analysis = new SampleVariantStatsAnalysis(executorParams, outDir).setSampleNames(sampleNames);
        AnalysisResult result = analysis.execute();
        System.out.println(result.toString());

        System.out.println("Results at: " + outDir);
    }

    @Test
    public void sampleStatsAnalysisByFamilyId() throws AnalysisException {
        executorParams.put("ID", "spark-parquet");

        SampleVariantStatsAnalysis analysis = new SampleVariantStatsAnalysis(executorParams, outDir).setFamilyId("FF");
        AnalysisResult result = analysis.execute();
        System.out.println(result.toString());

        System.out.println("Results at: " + outDir);
    }

    @Test
    public void sampleStatsAnalysisByIndividualId() throws AnalysisException {
        executorParams.put("ID", "spark-parquet");

        SampleVariantStatsAnalysis analysis = new SampleVariantStatsAnalysis(executorParams, outDir).setIndividualId(NA12877);
        AnalysisResult result = analysis.execute();
        System.out.println(result.toString());

        System.out.println("Results at: " + outDir);
    }
}