package org.opencb.oskar.analysis.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.oskar.analysis.OskarAnalysis;

import java.nio.file.Path;
import java.util.List;

public abstract class VariantStatsAnalysis extends OskarAnalysis {

    private Path outputFile;
    private String study;
    private String cohort;
    private List<String> samples;
    private Query variantsQuery;

    public VariantStatsAnalysis() {
    }

    public VariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsAnalysis(String cohort, ObjectMap executorParams, Path outDir) {
        setUp(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantStatsAnalysis{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", params=").append(params);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public VariantStatsAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public VariantStatsAnalysis setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public VariantStatsAnalysis setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public VariantStatsAnalysis setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public VariantStatsAnalysis setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    public Query getVariantsQuery() {
        return variantsQuery;
    }
}
