package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;

import java.nio.file.Path;

public abstract class CohortStatsExecutor extends OskarAnalysisExecutor {

    public CohortStatsExecutor() {
    }

    public CohortStatsExecutor(ObjectMap executorParams, Path outDir) {
        this.setup(executorParams, outDir);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortStatsExecutor{");
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }
}
