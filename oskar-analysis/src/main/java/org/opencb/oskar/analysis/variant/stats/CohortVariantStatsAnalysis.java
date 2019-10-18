package org.opencb.oskar.analysis.variant.stats;

import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = CohortVariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class CohortVariantStatsAnalysis extends OskarAnalysis {
    public static final String ID = "COHORT_STATS";

    private String study;
    private List<String> sampleNames;

    public CohortVariantStatsAnalysis() {
    }

    public String getStudy() {
        return study;
    }

    public CohortVariantStatsAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public CohortVariantStatsAnalysis setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    @Override
    protected void exec() throws AnalysisException {

        arm.startStep("Calculate");

        Path outputFile = outDir.resolve("cohort_stats.json");

        getAnalysisExecutor(CohortVariantStatsAnalysisExecutor.class)
                .setStudy(getStudy())
                .setOutputFile(outputFile)
                .setSampleNames(getSampleNames())
                .exec();

        arm.addFile(outputFile, FileResult.FileType.JSON);
        arm.endStep(100);
    }


}
