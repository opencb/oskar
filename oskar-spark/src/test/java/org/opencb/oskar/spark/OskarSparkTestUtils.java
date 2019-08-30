package org.opencb.oskar.spark;

import org.apache.log4j.Level;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.junit.rules.ExternalResource;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantStatsTransformer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.file_filter;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.file_qual;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.sample_data_field;

/**
 * Created on 07/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OskarSparkTestUtils extends ExternalResource {


    public static final String PLATINUM_STUDY = "hgvauser@platinum:illumina_platinum";

    public static final String NA12877 = "NA12877";
    public static final String NA12878 = "NA12878";
    public static final String NA12879 = "NA12879";
    public static final String NA12880 = "NA12880";
    public static final String NA12881 = "NA12881";
    public static final String NA12882 = "NA12882";
    public static final String NA12883 = "NA12883";
    public static final String NA12884 = "NA12884";
    public static final String NA12885 = "NA12885";
    public static final String NA12886 = "NA12886";
    public static final String NA12887 = "NA12887";
    public static final String NA12888 = "NA12888";
    public static final String NA12889 = "NA12889";
    public static final String NA12890 = "NA12890";
    public static final String NA12891 = "NA12891";
    public static final String NA12892 = "NA12892";
    public static final String NA12893 = "NA12893";


    public static final String PLATINUM_SMALL = "platinum_chr22.small.parquet";
    private static Path rootDir;
    private transient SparkSession spark;
    private Oskar oskar;

    @Override
    protected void before() throws Throwable {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        oskar = new Oskar(spark);

        org.apache.log4j.Logger.getLogger(ParquetReadSupport.class).setLevel(Level.WARN);
    }

    @Override
    protected void after() {
        spark.stop();
        spark = null;
        oskar = null;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public Oskar getOskar() {
        return oskar;
    }

    public Dataset<Row> getDummyDataset() {
        return spark.range(1, 10).toDF("value");
    }

    public static Path getRootDir() throws IOException {
        if (rootDir == null) {
            rootDir = Paths.get("target/test-data", "junit-opencga-storage-" +
                    new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
            Files.createDirectories(rootDir);
        }
        return rootDir;
    }

    public Dataset<Row> getVariantsDataset() throws OskarException, IOException {

        Path platinumSmall = getFile(PLATINUM_SMALL).toPath();
        getFile(PLATINUM_SMALL + ".meta.json.gz");

        return oskar.load(platinumSmall);
    }

    public File getFile(String name) throws IOException {
        Path path = getRootDir().resolve(name);
        File file = path.toFile();
        if (!file.exists()) {
            Files.copy(getClass().getClassLoader().getResourceAsStream(name), path, StandardCopyOption.REPLACE_EXISTING);
        }
        return file;
    }


    public Dataset<Row> toVcf(Dataset<Row> df) {
        return toVcf(df, oskar.metadata().samples(df).values().iterator().next());
    }

    public Dataset<Row> toVcf(Dataset<Row> df, String... samples) {
        return toVcf(df, Arrays.asList(samples));
    }

    public Dataset<Row> toVcf(Dataset<Row> df, List<String> samples) {

        List<Column> columns = new ArrayList<>();


        columns.add(col("chromosome").as("#CHROM"));
        columns.add(col("start").as("POS"));
        columns.add(coalesce(expr("annotation.id"), lit(".")).as("ID"));
        columns.add(when(col("reference").equalTo(""), lit("-")).otherwise(col("reference")).as("REF"));
//                when(col("alternate").equalTo(""), lit("-")).otherwise(col("alternate")).as("ALT"),
        columns.add(when(
                size(col("studies").apply(0).apply("secondaryAlternates")).gt(0),
                concat_ws(",",
                        array(
                                when(col("alternate").equalTo(""),
                                        lit("-"))
                                        .otherwise(col("alternate")),
                                concat_ws(",", col("studies").apply(0).apply("secondaryAlternates").apply("alternate")))))
                .otherwise(when(
                        col("alternate").equalTo(""),
                        lit("-"))
                        .otherwise(col("alternate"))).as("ALT"));

        columns.add(lit(".").as("QUAL"));
//        columns.add(file_qual("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").as("QUAL"));

        columns.add(lit("PASS").as("FILTER"));
//        columns.add(file_filter("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").as("FILTER"));
//                concat_ws(";", fileFilter("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).as("FILTER"),
//                concat(
//                        lit("AF="), expr("studies[0].stats['ALL'].altAlleleFreq"),
//                        lit(";AC="), expr("studies[0].stats['ALL'].altAlleleCount"),
//                        lit(";AN="), expr("studies[0].stats['ALL'].alleleCount")).as("INFO"),

        columns.add(map(
                lit("AF"), expr("studies[0].stats['ALL'].altAlleleFreq"),
                lit("AC"), expr("studies[0].stats['ALL'].altAlleleCount"),
                lit("AN"), expr("studies[0].stats['ALL'].alleleCount")).as("INFO"));
        columns.add(lit("GT").as("FORMAT"));

        for (String sample : samples) {
//            columns.add(sample_data_field("studies", sample, "GT").as(sample));
            columns.add(when(sample_data_field("studies", sample, "GT").equalTo("./."), "0/0").otherwise(sample_data_field("studies", sample, "GT")).as(sample));
        }

        df = new VariantStatsTransformer().setMissingAsReference(true).transform(df).select(columns.toArray(new Column[0]));

        return df;
    }

}
