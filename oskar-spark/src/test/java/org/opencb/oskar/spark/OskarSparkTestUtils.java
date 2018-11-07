package org.opencb.oskar.spark;

import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.junit.rules.ExternalResource;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;

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

    protected static final String PLATINUM_SMALL = "platinum_chr22.small.parquet";
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

        Files.copy(getClass().getClassLoader().getResourceAsStream(PLATINUM_SMALL),
                getRootDir().resolve(PLATINUM_SMALL), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(getClass().getClassLoader().getResourceAsStream(PLATINUM_SMALL + ".meta.json.gz"),
                getRootDir().resolve(PLATINUM_SMALL + ".meta.json.gz"), StandardCopyOption.REPLACE_EXISTING);

        return oskar.load(getRootDir().resolve(PLATINUM_SMALL));
    }

}
