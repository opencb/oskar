package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;

public class AbstractCohortStatsExecutor extends AbstractAnalysisExecutor {

    public AbstractCohortStatsExecutor() {
    }

    public AbstractCohortStatsExecutor(ObjectMap executorParams, Path outDir) {
        this.setup(executorParams, outDir);
    }

    protected void setup(ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
    }

    @Override
    public AnalysisResult exec() throws AnalysisException {
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractCohortStatsExecutor{");
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }
}
