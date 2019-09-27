package org.opencb.oskar.analysis.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;

import java.nio.file.Path;

public abstract class VariantStatsExecutor extends OskarAnalysisExecutor {

    private String cohort;

    public VariantStatsExecutor() {
    }

    public VariantStatsExecutor(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        setup(cohort, executorParams, outDir);
    }

    protected void setup(String cohort, ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
        this.cohort = StringUtils.isEmpty(cohort) ? "ALL" : cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantStatsExecutor{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getCohort() {
        return cohort;
    }

    public VariantStatsExecutor setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }
}
