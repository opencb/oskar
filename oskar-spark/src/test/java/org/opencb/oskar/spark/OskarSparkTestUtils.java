package org.opencb.oskar.spark;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.junit.rules.ExternalResource;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;

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

}
