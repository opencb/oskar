package org.opencb.oskar.analysis.variant.stats;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = SampleVariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class SampleVariantStatsAnalysis extends OskarAnalysis {

    public static final String ID = "SAMPLE_STATS";

    private String study;
    private List<String> sampleNames;
    private String individualId;
    private String familyId;

    public SampleVariantStatsAnalysis() {
    }

    public SampleVariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
        SampleVariantStatsAnalysisExecutor executor = getAnalysisExecutor(SampleVariantStatsAnalysisExecutor.class);

        executor.setUp(executorParams, outDir);
        executor.setOutputFile(getOutputFile());
        executor.setStudy(getStudy());
        if (CollectionUtils.isNotEmpty(sampleNames)) {
            executor.setSampleNames(sampleNames);
        } else if (StringUtils.isNotEmpty(familyId)) {
            executor.setFamilyId(familyId);
        } else if (StringUtils.isNotEmpty(individualId)) {
            executor.setIndividualId(individualId);
        } else {
            throw new AnalysisException("Invalid input parameters for variant sample stats analysis");
        }

        arm.startStep("sample-variant-stats");
        executor.exec();
        arm.endStep(100);
    }

    public Path getOutputFile() {
        return outDir.resolve("sample_variant_stats.json");
    }

    public String getStudy() {
        return study;
    }

    public SampleVariantStatsAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public SampleVariantStatsAnalysis setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public String getIndividualId() {
        return individualId;
    }

    public SampleVariantStatsAnalysis setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getFamilyId() {
        return familyId;
    }

    public SampleVariantStatsAnalysis setFamilyId(String familyId) {
        this.familyId = familyId;
        return this;
    }
}
