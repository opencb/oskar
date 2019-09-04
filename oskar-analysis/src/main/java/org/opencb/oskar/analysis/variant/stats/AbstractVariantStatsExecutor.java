package org.opencb.oskar.analysis.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;

public class AbstractVariantStatsExecutor extends AbstractAnalysisExecutor {

    private String cohort;

    public AbstractVariantStatsExecutor() {
    }

    public AbstractVariantStatsExecutor(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public AbstractVariantStatsExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        setup(cohort, executorParams, outDir);
    }

    protected void setup(String cohort, ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
        this.cohort = StringUtils.isEmpty(cohort) ? "ALL" : cohort;
    }

    @Override
    public AnalysisResult exec() throws AnalysisException {
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractVariantStatsExecutor{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getCohort() {
        return cohort;
    }

    public AbstractVariantStatsExecutor setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }
}
