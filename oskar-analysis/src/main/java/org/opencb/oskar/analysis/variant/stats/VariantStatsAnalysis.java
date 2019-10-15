package org.opencb.oskar.analysis.variant.stats;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = VariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class VariantStatsAnalysis extends OskarAnalysis {

    public static final String ID = "VARIANT_STATS";

    private String study;
    private String cohort;
    private List<String> samples;
    private Path outputFile;
    private Query variantsQuery;

    public VariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        this(null, executorParams, outDir);
    }

    public VariantStatsAnalysis(String cohort, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.cohort = cohort;
    }

    @Override
    protected void check() throws AnalysisException {
        super.check();

        if (StringUtils.isEmpty(cohort) && CollectionUtils.isEmpty(samples)) {
            cohort = StudyEntry.DEFAULT_COHORT;
        }

        if (StringUtils.isEmpty(cohort)) {
            outputFile = outDir.resolve("variant_stats.tsv");
        } else {
            outputFile = outDir.resolve("variant_stats_" + cohort + ".tsv");
        }

        if (variantsQuery == null) {
            variantsQuery = new Query();
        }
    }

    @Override
    public void exec() throws AnalysisException {
        VariantStatsAnalysisExecutor executor = getAnalysisExecutor(VariantStatsAnalysisExecutor.class);

        executor.setStudy(study)
                .setCohort(cohort)
                .setSamples(samples)
                .setOutputFile(outputFile)
                .setVariantsQuery(variantsQuery);
        executor.setUp(executorParams, outDir);

        arm.startStep("variant-stats");
        executor.exec();
        arm.endStep(100);

        if (outputFile.toFile().exists()) {
            arm.addFile(outputFile, FileResult.FileType.TAB_SEPARATED);
        } else {
            arm.addWarning("Output file not generated");
        }
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

    public Query getVariantsQuery() {
        return variantsQuery;
    }

    public VariantStatsAnalysis setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }
}
