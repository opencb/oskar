package org.opencb.oskar.analysis.variant.stats;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarExecutor;
import org.opencb.oskar.analysis.exceptions.ExecutionException;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

public abstract class CohortVariantStatsExecutor extends OskarExecutor {

    private String study;
    private List<String> sampleNames;
    private Path outputFile;

    public CohortVariantStatsExecutor() {
    }

    public CohortVariantStatsExecutor(ObjectMap executorParams, Path outDir) {
        this.setUp(executorParams, outDir);
    }

    public String getStudy() {
        return study;
    }

    public CohortVariantStatsExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleNames() {
        return sampleNames;
    }

    public CohortVariantStatsExecutor setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    public Path getOutputFile() {
        return outputFile == null ? outDir.resolve("cohort_stats.json") : outputFile;
    }

    public CohortVariantStatsExecutor setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    protected void writeStatsToFile(VariantSetStats stats) throws ExecutionException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        File outFilename = getOutputFile().toFile();
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectMapper.writer().writeValueAsString(stats));
            pw.close();
        } catch (Exception e) {
            throw new ExecutionException("Error writing output file: " + outFilename, e);
        }
    }
}
