package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = VariantStats.ID, data = Analysis.AnalysisData.VARIANT)
public class VariantStats extends AbstractAnalysis {

    public static final String ID = "VARIANT_STATS";

    private String cohort;

    public VariantStats(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStats(String cohort, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    public AnalysisResult execute() throws AnalysisException {
        return null;
    }
}
