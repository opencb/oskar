package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.result.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.AnalysisResultManager;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.CohortVariantStatsSparkParquetAnalysisExecutor;

import java.io.File;
import java.io.IOException;

import static org.opencb.oskar.spark.OskarSparkTestUtils.getRootDir;

public class CohortStatsSparkParquetAnalysisExecutorTest {
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
    }

    @Test
    public void cohortStats() throws IOException, AnalysisException {
        CohortVariantStatsSparkParquetAnalysisExecutor executor = new CohortVariantStatsSparkParquetAnalysisExecutor(executorParams,
                oskarSparkTestUtils.getRootDir().toAbsolutePath());
        AnalysisResultManager amr = new AnalysisResultManager(getRootDir()).init("", new ObjectMap());
        executor.init(amr);
        executor.exec();
        AnalysisResult analysisResult = amr.close();

        System.out.println("Cohort stats done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
        System.out.println(analysisResult.toString());
    }
}