package org.opencb.oskar.spark.variant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.analysis.ModeOfInheritanceTransformer;
import org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer;
import org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Oskar {

    private final SparkSession spark;
    private final VariantMetadataManager vmm;

    public Oskar() {
        this(null);
    }

    public Oskar(SparkSession spark) {
        this.spark = spark;
        if (spark != null) {
            new VariantUdfManager().loadVariantUdfs(spark);
        }
        vmm = new VariantMetadataManager();
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
        String metadataPath = vmm.getMetadataPath(path);

        if (Paths.get(metadataPath).toFile().exists()) {
            dataset = vmm.setVariantMetadata(dataset, metadataPath);
        }

        return dataset;
    }

    public VariantMetadataManager metadata() {
        return vmm;
    }

    public Dataset<Row> stats(Dataset<Row> df, String studyId, String cohort, List<String> samples) {
        return new VariantStatsTransformer(studyId, cohort, samples).transform(df);
    }

    public Dataset<Row> globalStats(Dataset<Row> df, String studyId, String fileId) {
        return new VariantSetStatsTransformer(studyId, fileId).transform(df);
    }

    /**
     * Filter variants that match a given Mode Of Inheritance pattern.
     *
     * @param df                    Input variants dataframe
     * @param modeOfInheritance     Mode of inheritance pattern
     * @param family                Family to apply the filter
     * @param phenotype             Phenotype to match the mode of inheritance
     * @return                      Filtered dataframe with the same schema
     */
    public Dataset<Row> modeOfInheritance(Dataset<Row> df, String modeOfInheritance, String family, String phenotype) {
        return new ModeOfInheritanceTransformer(modeOfInheritance, family, phenotype).transform(df);
    }

}
