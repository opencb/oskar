package org.opencb.oskar.analysis.variant.stats;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;

import java.nio.file.Path;
import java.util.List;

public abstract class SampleVariantStatsAnalysis extends OskarAnalysis {

    protected String study;
    protected List<String> sampleNames;
    protected String individualId;
    protected String familyId;
    private Path outputFile;

    public SampleVariantStatsAnalysis() {
    }

    public SampleVariantStatsAnalysis(ObjectMap executorParams, Path outDir) {
        super.setUp(executorParams, outDir);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleVariantStatsAnalysis{");
        sb.append("sampleNames=").append(sampleNames);
        sb.append(", individualId='").append(individualId).append('\'');
        sb.append(", familyId='").append(familyId).append('\'');
        sb.append(", params=").append(params);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
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

    public Path getOutputFile() {
        return outputFile;
    }

    public SampleVariantStatsAnalysis setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    protected void writeStatsToFile(List<SampleVariantStats> stats) throws OskarAnalysisException {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

        Path outFilename = getOutputFile();
        try {
            objectWriter.writeValue(outFilename.toFile(), stats);
        } catch (Exception e) {
            throw new OskarAnalysisException("Error writing output file: " + outFilename, e);
        }
    }
}
