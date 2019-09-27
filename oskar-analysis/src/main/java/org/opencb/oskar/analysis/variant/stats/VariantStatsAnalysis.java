package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = VariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class VariantStatsAnalysis extends OskarAnalysis {

    public static final String ID = "VARIANT_STATS";

    private String cohort;

    public VariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsAnalysis(String cohort, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    public void exec() throws AnalysisException {
    }
}
