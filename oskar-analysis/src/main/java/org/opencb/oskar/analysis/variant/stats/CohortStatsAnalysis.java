package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = CohortStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class CohortStatsAnalysis extends OskarAnalysis {
    public static final String ID = "COHORT_STATS";

    public CohortStatsAnalysis(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
    }
}
