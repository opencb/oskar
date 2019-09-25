package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = CohortStats.ID, data = Analysis.AnalysisData.VARIANT)
public class CohortStats extends AbstractAnalysis {

    public static final String ID = "COHORT_STATS";


    public CohortStats(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public AnalysisResult execute() throws AnalysisException {
        return null;
    }
}
