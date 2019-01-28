package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PCAAnalysisTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void pca() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        String studyId = "hgvauser@platinum:illumina_platinum";
        int k = 2;

        PCAAnalysis pca = new PCAAnalysis(df, studyId, k);
        PCAAnalysis.PCAAnalysisResult result = pca.execute();

        Matrix principalComponents = result.getPrincipalComponents();
        Matrix projection = result.getProjection();

        System.out.println("Principal components: num. rows = " + principalComponents.numRows()
                + ", num. cols = " + principalComponents.numCols());
        System.out.println("Projection: num. rows = " + projection.numRows() + ", num. cols = " + projection.numCols());

        Iterator<Vector> vectorIterator = projection.rowIter();
        while (vectorIterator.hasNext()) {
            System.out.println(vectorIterator.next());
        }
    }

    @Test
    public void pca1() {
        int k = 2;
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkTest.getSpark().sparkContext());

        List<Vector> data = Arrays.asList(
                Vectors.dense(1,2, 1, 3, 6),
                Vectors.dense(3,4, 2, 4, 7),
                Vectors.dense(5,6, 3, 5, 8)
        );

        JavaRDD<Vector> rows = jsc.parallelize(data);

        // Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(rows.rdd());

        System.out.println("Input matrix");
        for (Vector vector : mat.rows().toJavaRDD().collect()) {
            System.out.println(vector);
        }

        // Compute the top 4 principal components.
        // Principal components are stored in a local dense matrix.
        Matrix pc = mat.computePrincipalComponents(k);
        displayMatrix(pc, "PC matrix");

        // Project the rows to the linear space spanned by the top 4 principal components.
        RowMatrix projected = mat.multiply(pc);
        System.out.println("Projected matrix");
        for (Vector vector : projected.rows().toJavaRDD().collect()) {
            System.out.println(vector);
        }
    }

    private void displayMatrix(Matrix mat, String msg) {
        System.out.println("======= " + msg + " =======");
        Iterator<Vector> vectorIterator = mat.rowIter();
        while (vectorIterator.hasNext()) {
            System.out.println(vectorIterator.next());
        }
    }
}