package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.analysis.stats.ChiSquareTest;
import org.opencb.oskar.analysis.stats.ChiSquareTestResult;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;

public class ChiSquareTransformerTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testChiSquareFisherTransformer() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        new ChiSquareTransformer().setStudyId("hgvauser@platinum:illumina_platinum")
                .setPhenotype("KK")
                .transform(df)
//                .where("code != 0").show();
//                .where(col("code").notEqual(0))
                .show();
    }

    @Test
    public void testChiSquare() {
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
//        CHR  SNP         BP   A1      F_A      F_U   A2        CHISQ            P           OR
//        1 snp1          1    A   0.1667      0.5    C          1.5       0.2207          0.2
//        1 snp2          2    G   0.1667   0.6667    T        3.086      0.07898          0.1

        ChiSquareTestResult chiSquareTestResult;
        int a = 1; // case #REF
        int b = 3; // control #REF
        int c = 5; // case #ALT
        int d = 3; // control #ALT
        chiSquareTestResult = ChiSquareTest.chiSquareTest(a, b, c, d);
        System.out.println(chiSquareTestResult.toString());

        a = 1;
        b = 4;
        c = 5;
        d = 2;
        chiSquareTestResult = ChiSquareTest.chiSquareTest(a, b, c, d);
        System.out.println(chiSquareTestResult.toString());
    }
}