package org.opencb.oskar.spark.variant.analysis.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.models.variant.stats.VariantSampleStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.analysis.variant.stats.SampleStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.SampleStatsExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantSampleStatsTransformer;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = SampleStatsAnalysis.ID,
        source= AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class SampleStatsSparkParquetAnalysisExecutor extends SampleStatsExecutor {

    private Oskar oskar;

    private Dataset<Row> inputDataset;
    private String studyId;

    public SampleStatsSparkParquetAnalysisExecutor() {
    }

    public SampleStatsSparkParquetAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
        studyId = getExecutorParams().getString("STUDY_ID");
        String parquetFilename = getExecutorParams().getString("FILE");
        String master = getExecutorParams().getString("MASTER");

        // Prepare input dataset from the input parquet file
        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("sample stats")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        oskar = new Oskar(sparkSession);
        try {
            inputDataset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new AnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        // Call to the dataset transformer
        VariantSampleStatsTransformer transformer = new VariantSampleStatsTransformer();

        if (CollectionUtils.isEmpty(sampleNames)) {
            if (StringUtils.isNotEmpty(familyId)) {
                // Get sample names from family
                sampleNames = getSampleNamesByFamilyId(familyId);
            } else if (StringUtils.isNotEmpty(individualId)) {
                // Get sample names from individual
                sampleNames = getSampleNamesByIndividualId(individualId);
            } else {
                // This case should never occur (it is checked before calling)
                throw new AnalysisException("Invalid parameters: missing sample names, family ID or individual ID");
            }
        }

        Dataset<Row> outputDs = transformer.setSamples(sampleNames).transform(inputDataset);
        Map<String, VariantSampleStats> stats = VariantSampleStatsTransformer.toSampleStats(outputDs);

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

        String outFilename = getOutDir() + "/sample_stats.json";
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectWriter.writeValueAsString(stats));
            pw.close();
        } catch (Exception e) {
            throw new AnalysisException("Error writing output file: " + outFilename, e);
        }

        if (new File(outFilename).exists()) {
            arm.addFile(Paths.get(outFilename), FileResult.FileType.JSON);
        }
    }

    private List<String> getSampleNamesByFamilyId(String familyId) throws AnalysisException {
        Set<String> sampleNames = new HashSet<>();

        VariantMetadata variantMetadata = oskar.metadata().variantMetadata(inputDataset);
        for (VariantStudyMetadata study : variantMetadata.getStudies()) {
            if (studyId.equals(study.getId())) {
                for (Individual individual : study.getIndividuals()) {
                    if (StringUtils.isNotEmpty(familyId) && familyId.equals(individual.getFamily())) {
                        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                            for (Sample sample : individual.getSamples()) {
                                if (StringUtils.isNotEmpty(sample.getId())) {
                                    sampleNames.add(sample.getId());
                                }
                            }
                        }
                    }
                }
                break;
            }
        }

        // Sanity check
        if (CollectionUtils.isEmpty(sampleNames)) {
            throw new AnalysisException("Invalid parameters: no samples found for family ID '" + familyId + "'");
        }
        return sampleNames.stream().collect(Collectors.toList());
    }

    private List<String> getSampleNamesByIndividualId(String individualId) throws AnalysisException {
        List<String> sampleNames = new ArrayList<>();

        VariantMetadata variantMetadata = oskar.metadata().variantMetadata(inputDataset);
        for (VariantStudyMetadata study : variantMetadata.getStudies()) {
            if (studyId.equals(study.getId())) {
                for (Individual individual : study.getIndividuals()) {
                    if (StringUtils.isNotEmpty(individualId) && individualId.equals(individual.getId())) {
                        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                            for (Sample sample : individual.getSamples()) {
                                if (StringUtils.isNotEmpty(sample.getId())) {
                                    sampleNames.add(sample.getId());
                                }
                            }
                        }
                        break;
                    }
                }
                break;
            }
        }

        // Sanity check
        if (CollectionUtils.isEmpty(sampleNames)) {
            throw new AnalysisException("Invalid parameters: no samples found for individual ID '" + individualId + "'");
        }
        return sampleNames.stream().collect(Collectors.toList());
    }
}
