package org.opencb.oskar.analysis;

import org.junit.Test;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.core.annotations.Analysis;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.opencb.oskar.core.annotations.AnalysisExecutor.Framework;
import static org.opencb.oskar.core.annotations.AnalysisExecutor.Source;

public class OskarAnalysisTest {

    @AnalysisExecutor(id="test-executor", analysis = "test-analysis", framework = Framework.ITERATOR, source= Source.VCF_FILE)
    public static class MyExecutor1 extends OskarAnalysisExecutor { @Override public void exec() { } }

    @AnalysisExecutor(id="test-executor-mr", analysis = "test-analysis", framework = Framework.MAP_REDUCE, source= Source.HBASE)
    public static class MyExecutor2 extends OskarAnalysisExecutor { @Override public void exec() { } }

    @Analysis(id = "test-analysis", data = Analysis.AnalysisData.VARIANT)
    public static class MyAnalysis extends OskarAnalysis { @Override public void exec() { } }


    @Test
    public void testGetExecutorClass() throws AnalysisExecutorException {
        OskarAnalysis analysis = new MyAnalysis();

        assertEquals("test-analysis", analysis.getId());
        assertEquals(MyExecutor1.class, analysis.getAnalysisExecutorClass("test-executor"));
        assertEquals(MyExecutor2.class, analysis.getAnalysisExecutorClass("test-executor-mr"));

        analysis.setUp(null, Paths.get(""), Collections.singletonList(Source.HBASE), Arrays.asList(Framework.MAP_REDUCE, Framework.ITERATOR));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());

        analysis.setUp(null, Paths.get(""), Collections.singletonList(Source.VCF_FILE), Collections.singletonList(Framework.ITERATOR));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.setUp(null, Paths.get(""), Collections.singletonList(Source.VCF_FILE), Arrays.asList(Framework.MAP_REDUCE, Framework.ITERATOR));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.setUp(null, Paths.get(""), Arrays.asList(Source.VCF_FILE, Source.HBASE), Arrays.asList(Framework.ITERATOR, Framework.MAP_REDUCE));
        assertEquals("test-executor", analysis.getAnalysisExecutor().getId());

        analysis.setUp(null, Paths.get(""), Arrays.asList(Source.VCF_FILE, Source.HBASE), Arrays.asList(Framework.MAP_REDUCE, Framework.ITERATOR));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());

        analysis.setUp(null, Paths.get(""), Arrays.asList(Source.HBASE, Source.VCF_FILE), Arrays.asList(Framework.MAP_REDUCE, Framework.ITERATOR));
        assertEquals("test-executor-mr", analysis.getAnalysisExecutor().getId());
    }
}