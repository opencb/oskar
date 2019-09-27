package org.opencb.oskar.analysis.result;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class AnalysisResultManagerTest {

    private AnalysisResultManager arm;
    private static Path rootDir;

    @BeforeClass
    public static void beforeClass() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(rootDir);
    }

    @Before
    public void setUp() throws Exception {
        arm = new AnalysisResultManager(rootDir);
        arm.init("myTest", new ObjectMap());
    }

    @After
    public void tearDown() throws Exception {
        arm.close();
    }

    @Test
    public void testReadWrite() throws AnalysisException {

        arm.startStep("step1");
        arm.endStep(100);
        arm.addFile(rootDir.resolve("file1.txt").toAbsolutePath(), FileResult.FileType.TAB_SEPARATED);

    }
}