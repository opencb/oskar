package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = SampleStats.ID, data = Analysis.AnalysisData.VARIANT)
public class SampleStats  extends AbstractAnalysis {

    public static final String ID = "SAMPLE_STATS";

    private List<String> sampleNames;

    public SampleStats(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.sampleNames = sampleNames;
    }

    @Override
    public AnalysisResult execute() throws AnalysisException {
        return null;
    }
}
