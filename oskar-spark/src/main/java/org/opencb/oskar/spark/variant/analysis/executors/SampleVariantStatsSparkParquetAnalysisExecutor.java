package org.opencb.oskar.spark.variant.analysis.executors;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.SampleVariantStatsTransformer;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = SampleVariantStatsAnalysis.ID,
        source= AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class SampleVariantStatsSparkParquetAnalysisExecutor extends SampleVariantStatsExecutor {

    private Oskar oskar;

    private Dataset<Row> inputDataset;
    private String studyId;

    public SampleVariantStatsSparkParquetAnalysisExecutor() {
    }

    public SampleVariantStatsSparkParquetAnalysisExecutor(ObjectMap executorParams, Path outDir) {
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
        SampleVariantStatsTransformer transformer = new SampleVariantStatsTransformer();
        transformer.setStudyId(studyId);

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
        List<SampleVariantStats> stats = SampleVariantStatsTransformer.toSampleVariantStats(outputDs);

        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

        String outFilename = getOutDir() + "/sample_variant_stats.json";
        try {
            objectWriter.writeValue(new File(outFilename), stats);

//            PrintWriter pw = new PrintWriter(outFilename);
//            pw.println(new String(data));
////            pw.println(stats.get(0).toString());//objectWriter.writeValueAsString(stats));
//            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
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
