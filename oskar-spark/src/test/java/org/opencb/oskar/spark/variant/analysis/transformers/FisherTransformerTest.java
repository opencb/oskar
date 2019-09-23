package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.stats.FisherTestResult;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.biotypes;


public class FisherTransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void gg() throws IOException, OskarException {
        Integer[] indices = new Integer[3];
        indices[0] = 1;
        indices[1] = 3;
        indices[2] = 5;

        Dataset<Row> df = sparkTest.getVariantsDataset();
        df.select("id","chromosome", "type").withColumn("test", lit(indices)).show(false);
    }

    @Test
    public void kk() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        df.groupBy("annotation.consequenceTypes.biotype", "type").count().show(false);
    }

    @Test
    public void ii() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        Dataset<Row> df0 = df.withColumn("conservation", explode(col("annotation.conservation")))
                .selectExpr("*", "conservation.*").filter("source = 'gerp'")
                .withColumnRenamed("score", "conservationScore")
                .withColumnRenamed("source", "conservationSource")
                .groupBy("type", "conservationScore")
                .count()
                .orderBy("type", "conservationScore")
                ;

        double[] splits = {Double.NEGATIVE_INFINITY, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, Double.POSITIVE_INFINITY};
        Bucketizer bucketizer = new Bucketizer()
                .setInputCol("conservationScore")
                .setOutputCol("range")
                .setSplits(splits);

        Dataset<Row> df1 = df0.filter("type = 'INDEL'");
        Dataset<Row> bucketedData1 = bucketizer.transform(df1).groupBy("range").count().orderBy("range")
                .withColumn("type", lit("INDEL"));

        Dataset<Row> df2 = df0.filter("type = 'SNV'");
        Dataset<Row> bucketedData2 = bucketizer.transform(df2).groupBy("range").count().orderBy("range")
                .withColumn("type", lit("SNV"));

        Dataset<Row> df00 = bucketedData1.union(bucketedData2);
        df00.show();

//        df.withColumn("biotype", explode(biotypes("annotation")))
//                .withColumn("conservationSource", explode(col("annotation.conservation.source")))
//                .withColumn("conservationScore", explode(col("annotation.conservation.score")))
//                .show();
        //.groupBy("annotation.consequenceTypes.biotype", "type").count().show(false);
    }

    @Test
    public void nn() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        Dataset<Row> df0 =
                df.withColumn("ct", explode(col("annotation.consequenceTypes")))
                        .selectExpr("id",/* "chromosome", "start", "end", "reference", "alternate", "strand",*/
                                "annotation.id as dbSNP",
                                "ct.geneName as geneName",
                                "ct.ensemblGeneId as ensemblGeneId",
                                "ct.ensemblTranscriptId as ensemblTranscriptId",
                                "ct.biotype as biotype",
                                "ct.sequenceOntologyTerms.name as SO")
//                        .drop("ct")
                ;

        int i = 0;
        Iterator<Row> rowIterator = df0.toLocalIterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            List<Object> soList = row.getList(row.fieldIndex("SO"));
            System.out.println(row.get(0) + " " + row.get(1));
            for (Object so : soList) {
                System.out.println("\t" + so);

            }
            if (++i >= 10) break;
        }
//        df0.show(100,false);
    }


    @Test
    public void jj() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();
        Dataset<Row> df0 =
                df.withColumn("conservation", explode(col("annotation.conservation")))
                        .withColumn("functionalScore", explode(col("annotation.functionalScore")))
                        .selectExpr("id", "chromosome", "start", "end", "reference", "alternate", "strand", "type",
                                "conservation.source as conservationSource",
                                "conservation.score as conservationScore",
                                "functionalScore.source as functionalSource",
                                "functionalScore.score as functionalScore")
                        .drop("conservation", "functionalScore")
//                        .filter("source = 'gerp'")
//                .withColumnRenamed("score", "conservationScore")
//                .withColumnRenamed("source", "conservationSource")
////                .withColumn("biotypes", col("annotation.consequenceTypes.biotype"))
//                .withColumn("biotype", explode(biotypes("annotation")))
//                .groupBy("type", "biotype", "conservationScore").count()
//                        .filter("id = '22:16149692:G:T'")
//                        .show(100)
//                .groupBy("type", "conservationScore")
//                .count()
//                .orderBy("type", "conservationScore")
                ;

        df0.show(false);

//        System.out.println("Number of biotypes = " + df0.select("biotype").distinct().collectAsList().size());
        //System.out.println(StringUtils.join(df0.select("type").distinct().collectAsList(), ","));
        //System.out.println(StringUtils.join(df0.select("biotype").distinct().collectAsList(), ","));

//        df0.show(false);
//        double[] splits = {Double.NEGATIVE_INFINITY, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, Double.POSITIVE_INFINITY};
//        Bucketizer bucketizer = new Bucketizer()
//                .setInputCol("conservationScore")
//                .setOutputCol("range")
//                .setSplits(splits);
//
//        Dataset<Row> df1 = df0.filter("type = 'INDEL'");
//        Dataset<Row> bucketedData1 = bucketizer.transform(df1).groupBy("range").count().orderBy("range")
//                .withColumn("type", functions.lit("INDEL"));
//
//        Dataset<Row> df2 = df0.filter("type = 'SNV'");
//        Dataset<Row> bucketedData2 = bucketizer.transform(df2).groupBy("range").count().orderBy("range")
//                .withColumn("type", functions.lit("SNV"));
//
//        Dataset<Row> df00 = bucketedData1.union(bucketedData2);
//        df00.show();
    }

    @Test
    public void oo() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        df.withColumn("biotype", explode(biotypes("annotation"))).filter("type != 'SNV'")
                .select("type", "biotype").groupBy("type", "biotype").count()
                .orderBy("type", "biotype").show();
    }

    @Test
    public void tt() throws IOException, OskarException {
        int chunkSize = 1000000;

        Dataset<Row> df = sparkTest.getVariantsDataset();


        df.withColumn("biotype", explode(biotypes("annotation")));
//                .select("type", "biotype").groupBy("type", "biotype").count()
//                .orderBy("type", "biotype").show();

//        df.withColumn("new", explode(col("annotation.conservation"))).select("new")
//                .withColumn("score", element_at(col("new"), 0)).show(false);

//        df.withColumn("biotype", explode(col("annotation.consequenceTypes.biotype")))
//                .filter("biotype != ''").groupBy("type", "biotype").count()
//                .orderBy("type", "biotype").show();


//        df.select(populationFrequency("annotation", "1kG_phase3", "ALL").as("pf")).filter(col("pf").gt(0)).show();

//        df.withColumn("biotype", explode(biotypes("annotation")))
//                .select("type", "biotype").groupBy("type", "biotype").count()
//                .orderBy("type", "biotype").show();

//        df.select("type", explode(biotypes("annotation")).alias("biotype")).groupBy("type", "biotype").count().orderBy("type", "biotype").show();

//        df.select(col("start").divide(chunkSize).cast(DataTypes.IntegerType).multiply(chunkSize).alias("rangeIdx"))
//                .groupBy("rangeIdx").count().orderBy("rangeIdx").show();

//        return dataset
//                .select(col(inputCol)
//                        .divide(step)
//                        .cast(DataTypes.IntegerType)
//                        .multiply(step)
//                        .alias(inputCol))
//                .groupBy(inputCol)
//                .count()
//                .orderBy(inputCol);


//        int chromLength = 45000000;
//        int chunkSize = 1000000;
//        double[] splits = new double[chromLength / chunkSize + 1];
//        for (int j=0, i = 1; i < chromLength; i += chunkSize, j++) {
//            splits[j] = i;
//        }
//        splits[splits.length - 1] = Double.POSITIVE_INFINITY;
//
//        //= {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, 1.0, 1.5, Double.POSITIVE_INFINITY};
//
////        List<Row> data = Arrays.asList(
////                RowFactory.create(-999.9),
////                RowFactory.create(-0.5),
////                RowFactory.create(0.1),
////                RowFactory.create(-0.3),
////                RowFactory.create(0.0),
////                RowFactory.create(0.3),
////                RowFactory.create(0.2),
////                RowFactory.create(999.9)
////        );
////        StructType schema = new StructType(new StructField[]{
////                new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
////        });
////        Dataset<Row> dataFrame = sparkTest.getSpark().createDataFrame(data, schema);
//
//        Bucketizer bucketizer = new Bucketizer()
//                .setInputCol("start")
//                .setOutputCol("range")
//                .setSplits(splits);
//
//        // Transform original data into its bucket index.
//        System.out.println("Bucketizer output with " + (bucketizer.getSplits().length-1) + " buckets");
//
//        Dataset<Row> df = sparkTest.getVariantsDataset();
//        Dataset<Row> bucketedData = bucketizer.transform(df);
//        //bucketedData.show();
//
//        Dataset<Row> count = bucketedData.groupBy("range").count();//.filter("count > 0");
//
//        bucketedData.show();
//        count.sort("range").show();
    }

    @Test
    public void testFisherTransformer() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        new FisherTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype("JJ")
                .transform(df)
//                .where("code != 0").show();
//                .where(col("code").notEqual(0))
                .show();
    }

    @Test
    public void testFisher() {
//        Plink
//
//        $ cat test.map
//        1 snp1 0 1
//        1 snp2 0 2
//
//        $ cat test.ped
//        Family ID   Individual ID   Paternal ID   Maternal ID   Sex(1=male, 2=female)   Phenotye   Genotypes...
//        1 1 0 0 1  1  A A  G T
//        2 1 0 0 1  1  A C  T G
//        3 1 0 0 1  1  C C  G G
//        4 1 0 0 1  2  A C  T T
//        5 1 0 0 1  2  C C  G T
//        6 1 0 0 1  2  C C  T T
//
//        Plink result:
//        CHR  SNP         BP   A1      F_A      F_U   A2            P           OR
//        1 snp1          1    A   0.1667      0.5    C       0.5455          0.2
//        1 snp2          2    G   0.1667   0.6667    T       0.2424          0.1

        int mode = FisherExactTest.TWO_SIDED;
        FisherTestResult fisherTestResult;
        FisherExactTest fisherExactTest = new FisherExactTest();
        int a = 1; // case #REF
        int b = 3; // control #REF
        int c = 5; // case #ALT
        int d = 3; // control #ALT
        fisherTestResult = fisherExactTest.fisherTest(a, b, c, d, mode);
        System.out.println(fisherTestResult.toString());

        a = 1;
        b = 4;
        c = 5;
        d = 2;
        fisherTestResult = fisherExactTest.fisherTest(a, b, c, d, mode);
        System.out.println(fisherTestResult.toString());
    }
}