package org.opencb.oskar.spark.variant.analysis.executors;

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
import org.opencb.oskar.analysis.exceptions.ExecutionException;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.SampleVariantStatsTransformer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SampleVariantStatsSparkParquetExecutor extends SampleVariantStatsExecutor implements SparkParquetExecutor {

    private Oskar oskar;

    private Dataset<Row> inputDataset;
    private String studyId;

    public SampleVariantStatsSparkParquetExecutor() {
    }

    public SampleVariantStatsSparkParquetExecutor(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws ExecutionException {
        String parquetFilename = getFile();
        studyId = getStudy();
        SparkSession sparkSession = getSparkSession("sample variant stats");

        oskar = new Oskar(sparkSession);
        try {
            inputDataset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new ExecutionException("Error loading Parquet file: " + parquetFilename, e);
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
                throw new ExecutionException("Invalid parameters: missing sample names, family ID or individual ID");
            }
        }

        Dataset<Row> outputDs = transformer.setSamples(sampleNames).transform(inputDataset);
        List<SampleVariantStats> stats = SampleVariantStatsTransformer.toSampleVariantStats(outputDs);

        writeStatsToFile(stats);
    }

    private List<String> getSampleNamesByFamilyId(String familyId) throws ExecutionException {
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
            throw new ExecutionException("Invalid parameters: no samples found for family ID '" + familyId + "'");
        }
        return sampleNames.stream().collect(Collectors.toList());
    }

    private List<String> getSampleNamesByIndividualId(String individualId) throws ExecutionException {
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
            throw new ExecutionException("Invalid parameters: no samples found for individual ID '" + individualId + "'");
        }
        return sampleNames.stream().collect(Collectors.toList());
    }
}
