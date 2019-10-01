package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;

import java.nio.file.Path;
import java.util.List;

public abstract class SampleStatsExecutor extends OskarAnalysisExecutor {

    protected List<String> sampleNames;
    protected String individualId;
    protected String familyId;

    public SampleStatsExecutor() {
    }

    public SampleStatsExecutor(ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleStatsExecutor{");
        sb.append("sampleNames=").append(sampleNames);
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", familyId='").append(familyId).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append(", arm=").append(arm);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public SampleStatsExecutor setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleStatsExecutor setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public SampleStatsExecutor setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }
}
