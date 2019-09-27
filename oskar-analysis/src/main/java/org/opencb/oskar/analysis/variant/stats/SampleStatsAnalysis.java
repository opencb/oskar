package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = SampleStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class SampleStatsAnalysis extends OskarAnalysis {

    public static final String ID = "SAMPLE_STATS";

    private List<String> sampleNames;

    public SampleStatsAnalysis(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.sampleNames = sampleNames;
    }

    @Override
    public void exec() throws AnalysisException {
    }
}
