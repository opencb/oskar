package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.ExecutionException;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsExecutor;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.SampleVariantStatsSparkParquetExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

public class SampleStatsSparkParquetExecutorTest {
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
        executorParams.put("MASTER", "local[*]");
        executorParams.put("FILE", file.getAbsolutePath());

        sampleNames = new ArrayList<>();
        sampleNames.add(NA12877);
        sampleNames.add(NA12879);
        sampleNames.add(NA12885);
        sampleNames.add(NA12890);
    }

    @Test
    public void sampleStatsExecutorBySampleList() throws IOException, ExecutionException {
        SampleVariantStatsExecutor executor = new SampleVariantStatsSparkParquetExecutor(executorParams, outDir)
                .setStudy(OskarSparkTestUtils.PLATINUM_STUDY)
                .setSampleNames(sampleNames)
                .setOutputFile(getRootDir().resolve("sample_stats.json"));

        executor.exec();

        System.out.println("Sample stats done! Results at " + outDir);
    }

    @Test
    public void sampleStatsExecutorByFamilyId() throws IOException, ExecutionException {
        SampleVariantStatsExecutor executor = new SampleVariantStatsSparkParquetExecutor(executorParams, outDir)
                .setStudy(OskarSparkTestUtils.PLATINUM_STUDY)
                .setFamilyId("FF")
                .setOutputFile(getRootDir().resolve("sample_stats.json"));

        executor.exec();

        System.out.println("Sample stats done! Results at " + outDir);
    }

    @Test
    public void sampleStatsExecutorByIndividualId() throws IOException, ExecutionException {
        SampleVariantStatsExecutor executor = new SampleVariantStatsSparkParquetExecutor(executorParams, outDir)
                .setStudy(OskarSparkTestUtils.PLATINUM_STUDY)
                .setIndividualId(NA12877)
                .setOutputFile(getRootDir().resolve("sample_stats.json"));

        executor.exec();

        System.out.println("Sample stats done! Results at " + outDir);
    }

}