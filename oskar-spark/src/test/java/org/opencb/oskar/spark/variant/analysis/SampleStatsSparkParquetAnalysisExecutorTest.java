package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.spark.OskarSparkTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.oskar.spark.OskarSparkTestUtils.*;

public class SampleStatsSparkParquetAnalysisExecutorTest {
    private List<String> sampleNames;
    private String cohort;
    private ObjectMap executorParams;

    private OskarSparkTestUtils oskarSparkTestUtils;

    @Before
    public void init() throws IOException {
        // Prepare parquet and metadata test files
        oskarSparkTestUtils = new OskarSparkTestUtils();
        File file = oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL);
        oskarSparkTestUtils.getFile(OskarSparkTestUtils.PLATINUM_SMALL + ".meta.json.gz");

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
    public void sampleStats() throws IOException, AnalysisException {
        SampleStatsSparkParquetAnalysisExecutor executor = new SampleStatsSparkParquetAnalysisExecutor(sampleNames, executorParams,
                oskarSparkTestUtils.getRootDir().toAbsolutePath());
        AnalysisResult analysisResult = executor.exec();

        System.out.println("Sample stats done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }
}