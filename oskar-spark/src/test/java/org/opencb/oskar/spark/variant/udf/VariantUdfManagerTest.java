package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.*;

/**
 * Created on 07/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantUdfManagerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        spark = sparkTest.getSpark();
    }

    @Test
    public void testMl() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();


        new SQLTransformer().setStatement("select *,population_frequency(annotation, '1kG_phase3','ALL') as 1kg from __THIS__").transform(df).show();


        SQLTransformer sql = new SQLTransformer().setStatement("select *,population_frequency(annotation, '1kG_phase3','ALL') as 1kg from __THIS__");
        Bucketizer bucket = new Bucketizer().setInputCol("1kg").setOutputCol("bucket").setSplits(new double[]{Double.NEGATIVE_INFINITY, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 2});


        bucket.transform(sql.transform(df)).show(10);

    }

    @Test
    public void testUDFs() throws Exception {
        Dataset<Row> df = sparkTest.getVariantsDataset();

//        df.select(population_frequency("annotation", "1kG_phase3", "ALL").as("pf")).filter(col("pf").gt(0)).show();
//        df.select(population_frequency_as_map("annotation").apply("1kG_phase3:ALL").as("pf")).filter(col("pf").gt(0)).show();
//        df.select(genes("annotation")).show();
//        df.select(sample_data("studies", "NA12877").as("NA12877")).show();
        df.select(sample_data_field("studies", "NA12877", "GT").as("NA12877")).show();
        df.select(sample_data_field("studies", "NA12878", "GT").as("NA12878")).show();
        df.selectExpr("sample_data_field(studies, 'NA12877', 'GT') AS NA12877").show();
        df.selectExpr("sample_data_field(studies, 'NA12878', 'GT') AS NA12878").show();
    }

    @Test
    public void testPrintVcf() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        sparkTest.toVcf(df).show();
    }

    @Test
    public void testSql() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        df.createOrReplaceTempView("chr22");
        df.printSchema();

        spark.sql("SELECT " +
                "chromosome,start,end,reference,alternate,type," +
                "studies[0].format as FORMAT," +
                "studies[0].samplesData[0] as NA12877," +
                "studies[0].files[0].attributes.FILTER as FILTER," +
                "studies[0].files[0].attributes.QUAL as QUAL," +
                "map_values(studies[0].files[0].attributes) as INFO " +
                "FROM chr22 " +
                "LIMIT 10").show();

        spark.sql("SELECT " +
                "population_frequency_as_map(annotation)," +
                "chromosome,start,end,reference,alternate,type," +
                "studies[0].format as FORMAT," +
                "studies[0].samplesData[0] as NA12877," +
                "studies[0].files[0].attributes.FILTER as FILTER," +
                "studies[0].files[0].attributes.QUAL as QUAL," +
                "map_values(studies[0].files[0].attributes) as INFO " +
                "FROM chr22 " +
                "LIMIT 10").show(false);


        spark.sql("SELECT " +
                "consequence_types(annotation)," +
                "chromosome,start,reference,alternate," +
                "population_frequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE population_frequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.01 " +
                "LIMIT 10 ").show(false);

        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequence_types_by_gene(annotation, 'MICAL3')," +
                "population_frequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE consequence_types_by_gene(annotation, 'MICAL3')[0] is not null " +
                "limit 10 ").show(false);

        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequence_types_by_gene(annotation, 'MICAL3')," +
                "population_frequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE array_contains(annotation.consequenceTypes.geneName,'MICAL3') " +
                "AND population_frequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.1 " +
                "limit 10 ").show(false);



        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequence_types_by_gene(annotation, 'MICAL3')," +
                "genes(annotation)," +
                "population_frequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE " +
                "array_contains(genes(annotation),'MICAL3') " +
                "AND population_frequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.1 " +
                "limit 10 ").show(false);

    }

}