package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;
import org.opencb.oskar.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.VariantStatsSparkParquetAnalysis;

import java.io.File;
import java.io.IOException;

public class VariantStatsSparkParquetAnalysisTest {
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
    public void variantStats() throws IOException, OskarAnalysisException {
        VariantStatsAnalysis executor = new VariantStatsSparkParquetAnalysis(null, executorParams,
                oskarSparkTestUtils.getRootDir().toAbsolutePath())
                .setOutputFile(oskarSparkTestUtils.getRootDir().resolve("variants.tsv"));
        executor.exec();

        System.out.println("Variant stats done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
    }
}