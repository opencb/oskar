package org.opencb.oskar.spark.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.utils.FileUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer;
import org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer;
import org.opencb.oskar.spark.commons.converters.DataTypeUtils;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Oskar {

    private final SparkSession spark;

    public Oskar(SparkSession spark) {
        this.spark = spark;
        new VariantUdfManager().loadVariantUdfs(spark);
    }

    public Dataset<Row> load(Path path) throws OskarException {
        return load(path.toAbsolutePath().toString());
    }

    public Dataset<Row> load(String path) throws OskarException {
        Dataset<Row> dataset;
        if (path.endsWith("avro") || path.endsWith("avro.gz")) {
            // Do not fail if the file extension is "avro.gz"
            spark.sparkContext().hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");

            dataset = spark.read().format("com.databricks.spark.avro").load(path);
        } else if (path.endsWith("parquet")) {
            dataset = spark.read().format("parquet").load(path);
        } else {
            throw OskarException.unsupportedFileFormat(path);
        }

        // Read and add metadata
        String metadataPath = path + ".meta.json.gz";

        if (Paths.get(metadataPath).toFile().exists()) {
            VariantMetadata variantMetadata = loadMetadata(metadataPath);
            dataset = addVariantMetadata(dataset, variantMetadata);
        }

        return dataset;
    }

    public VariantMetadata loadMetadata(String path) throws OskarException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream is = FileUtils.newInputStream(Paths.get(path))) {
            return objectMapper.readValue(is, VariantMetadata.class);
        } catch (IOException e) {
            throw OskarException.errorLoadingVariantMetadataFile(e, path);
        }
    }

    /**
     * Writes the VariantMetadata into the schema metadata from the given dataset.
     *
     * @param dataset Dataset to modify
     * @param variantMetadata VariantMetadata to add
     * @return  Modified dataset
     */
    public Dataset<Row> addVariantMetadata(Dataset<Row> dataset, VariantMetadata variantMetadata) {
        Metadata metadata = createDatasetMetadata(variantMetadata);

        ArrayType studiesArrayType = (ArrayType) dataset.schema().apply("studies").dataType();
        StructType studyStructType = ((StructType) studiesArrayType.elementType());

        // Add metadata to samplesData field
        StructField samplesDataSchemaWithMetadata = DataTypeUtils.addMetadata(metadata, studyStructType.apply("samplesData"));

        // Replace samplesData field
        StructType elementType = DataTypeUtils.replaceField(studyStructType, samplesDataSchemaWithMetadata);

        return dataset.withColumn("studies", col("studies").as("studies", metadata))
                .withColumn("studies", col("studies").cast(new ArrayType(elementType, studiesArrayType.containsNull())));
    }

    private Metadata createDatasetMetadata(VariantMetadata variantMetadata) {
        Map<String, List<String>> samplesMap = new HashMap<>();
        for (VariantStudyMetadata study : variantMetadata.getStudies()) {
            List<String> samples = new ArrayList<>();
            for (Individual individual : study.getIndividuals()) {
                for (Sample sample : individual.getSamples()) {
                    samples.add(sample.getId());
                }
            }
            samplesMap.put(study.getId(), samples);
        }

        MetadataBuilder samplesMetadata = new MetadataBuilder();
        for (Map.Entry<String, List<String>> entry : samplesMap.entrySet()) {
            samplesMetadata.putStringArray(entry.getKey(), entry.getValue().toArray(new String[0]));
        }

        return new MetadataBuilder()
                .putMetadata("samples", samplesMetadata.build())
                .build();
    }

    public Dataset<Row> stats(Dataset<Row> df, String studyId, String cohort, List<String> samples) {
        return new VariantStatsTransformer(studyId, cohort, samples).transform(df);
    }

    public Dataset<Row> globalStats(Dataset<Row> df, String studyId, String fileId) {
        return new VariantSetStatsTransformer(studyId, fileId).transform(df);
    }

}
