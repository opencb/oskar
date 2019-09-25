package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;
import java.util.List;

public class AbstractSampleStatsExecutor extends AbstractAnalysisExecutor {

    protected List<String> sampleNames;
    protected String cohortName;

    public AbstractSampleStatsExecutor() {
    }

    public AbstractSampleStatsExecutor(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        this.setup(sampleNames, executorParams, outDir);
    }

    public AbstractSampleStatsExecutor(String cohortName, ObjectMap executorParams, Path outDir) {
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
    public AnalysisResult exec() throws AnalysisException {
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractSampleStatsExecutor{");
        sb.append("sampleNames=").append(sampleNames);
        sb.append(", cohortName='").append(cohortName).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }
}
