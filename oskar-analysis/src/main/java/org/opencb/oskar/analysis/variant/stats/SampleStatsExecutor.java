package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;

import java.nio.file.Path;
import java.util.List;

public abstract class SampleStatsExecutor extends OskarAnalysisExecutor {

    protected List<String> sampleNames;
    protected String cohortName;

    public SampleStatsExecutor() {
    }

    public SampleStatsExecutor(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        this.setup(sampleNames, executorParams, outDir);
    }

    public SampleStatsExecutor(String cohortName, ObjectMap executorParams, Path outDir) {
        this.setup(sampleNames, executorParams, outDir);
    }

    protected void setup(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
        this.sampleNames = sampleNames;
    }

    protected void setup(String cohortName, ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
        this.cohortName = cohortName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleStatsExecutor{");
        sb.append("sampleNames=").append(sampleNames);
        sb.append(", cohortName='").append(cohortName).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }
}
