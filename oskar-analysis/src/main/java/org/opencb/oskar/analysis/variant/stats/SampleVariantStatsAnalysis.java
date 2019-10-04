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

    private List<String> sampleNames;
    private String individualId;
    private String familyId;

    public SampleVariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
        SampleVariantStatsExecutor executor = getAnalysisExecutor(SampleVariantStatsExecutor.class, executorParams.getString("ID"));

        executor.setup(executorParams, outDir);
        if (CollectionUtils.isNotEmpty(sampleNames)) {
            executor.setSampleNames(sampleNames);
        } else if (StringUtils.isNotEmpty(familyId)) {
            executor.setFamilyId(familyId);
        } else if (StringUtils.isNotEmpty(individualId)) {
            executor.setIndividualId(individualId);
        } else {
            throw new AnalysisException("Invalid input parameters for variant sample stats analysis");
        }

        arm.startStep("variant-sample-stats");
        executor.exec();
        arm.endStep(100);
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
