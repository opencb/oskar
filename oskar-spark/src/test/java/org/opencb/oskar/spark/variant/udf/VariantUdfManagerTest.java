package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

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


        new SQLTransformer().setStatement("select *,populationFrequency(annotation, '1kG_phase3','ALL') as 1kg from __THIS__").transform(df).show();


        SQLTransformer sql = new SQLTransformer().setStatement("select *,populationFrequency(annotation, '1kG_phase3','ALL') as 1kg from __THIS__");
        Bucketizer bucket = new Bucketizer().setInputCol("1kg").setOutputCol("bucket").setSplits(new double[]{Double.NEGATIVE_INFINITY, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 2});


        bucket.transform(sql.transform(df)).show(10);

    }


    @Test
    public void testUdf() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();


        UserDefinedFunction mode = udf(new RowStringAbstractFunction1(), DataTypes.StringType);

        // Transform single column
        df.select(mode.apply(new ListBuffer<Column>().$plus$eq(col("annotation")))).show();
        // Add new column
        df.withColumn("new_col", mode.apply(new ListBuffer<Column>().$plus$eq(col("annotation")))).show();
        // Replace column
        df.withColumn("annotation", mode.apply(new ListBuffer<Column>().$plus$eq(col("annotation")))).show();

    }

    @Test
    public void testUDFs() throws Exception {

        Dataset<Row> df = sparkTest.getVariantsDataset();

        df.select(VariantUdfManager.populationFrequency("annotation", "1kG_phase3", "ALL")).show();
        df.select(VariantUdfManager.genes("annotation")).show();
        df.select(VariantUdfManager.sampleData("studies", "NA12877").as("NA12877")).show();
        df.select(VariantUdfManager.sampleDataField("studies", "NA12877", "GT").as("NA12877")).show();

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
                "populationFrequencyAsMap(annotation)," +
                "chromosome,start,end,reference,alternate,type," +
                "studies[0].format as FORMAT," +
                "studies[0].samplesData[0] as NA12877," +
                "studies[0].files[0].attributes.FILTER as FILTER," +
                "studies[0].files[0].attributes.QUAL as QUAL," +
                "map_values(studies[0].files[0].attributes) as INFO " +
                "FROM chr22 " +
                "LIMIT 10").show(false);


        spark.sql("SELECT " +
                "consequenceTypes(annotation)," +
                "chromosome,start,reference,alternate," +
                "populationFrequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE populationFrequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.01 " +
                "LIMIT 10 ").show(false);

        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequenceTypesByGene(annotation, 'MICAL3')," +
                "populationFrequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE consequenceTypesByGene(annotation, 'MICAL3')[0] is not null " +
                "limit 10 ").show(false);

        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequenceTypesByGene(annotation, 'MICAL3')," +
                "populationFrequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE array_contains(annotation.consequenceTypes.geneName,'MICAL3') " +
                "AND populationFrequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.1 " +
                "limit 10 ").show(false);



        spark.sql("SELECT " +
                "chromosome,start,reference,alternate," +
                "consequenceTypesByGene(annotation, 'MICAL3')," +
                "genes(annotation)," +
                "populationFrequency(annotation, '1kG_phase3','ALL') as 1kG_phase3_ALL " +
                "FROM chr22 " +
                "WHERE " +
                "array_contains(genes(annotation),'MICAL3') " +
                "AND populationFrequency(annotation, '1kG_phase3','ALL') between 0.000001 and 0.1 " +
                "limit 10 ").show(false);

    }

    public static class RowStringAbstractFunction1 extends AbstractFunction1<Row, String> implements Serializable {
        @Override
        public String apply(Row ss) {
            return "LLL";
        }
    }
}