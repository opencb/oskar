package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.oskar.analysis.OskarExecutor;

import java.nio.file.Path;
import java.util.List;

public abstract class VariantStatsExecutor extends OskarExecutor {

    private Path outputFile;
    private String study;
    private String cohort;
    private List<String> samples;
    private Query variantsQuery;

    public VariantStatsExecutor() {
    }

    public VariantStatsExecutor(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        setUp(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantStatsExecutor{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", params=").append(params);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public VariantStatsExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public VariantStatsExecutor setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public VariantStatsExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public VariantStatsExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public VariantStatsExecutor setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    public Query getVariantsQuery() {
        return variantsQuery;
    }
}
