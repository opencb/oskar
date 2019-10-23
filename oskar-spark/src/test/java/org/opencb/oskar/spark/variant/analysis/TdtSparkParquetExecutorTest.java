package org.opencb.oskar.spark.variant.analysis;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.ExecutionException;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.variant.analysis.executors.TdtSparkParquetExecutor;

import java.io.File;
import java.io.IOException;

public class TdtSparkParquetExecutorTest {

    private String phenotype;
    private ObjectMap executorParams;

    private OskarSparkTestUtils oskarSparkTestUtils;

    @Before
    public void init() throws IOException {
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
    }

    @Test
    public void tdt() throws IOException, ExecutionException {
        new TdtSparkParquetExecutor(phenotype, executorParams, oskarSparkTestUtils.getRootDir().toAbsolutePath())
                .setStudy(OskarSparkTestUtils.PLATINUM_STUDY)
                .exec();

        System.out.println("TDT done! Results at " + oskarSparkTestUtils.getRootDir().toAbsolutePath());
    }
}