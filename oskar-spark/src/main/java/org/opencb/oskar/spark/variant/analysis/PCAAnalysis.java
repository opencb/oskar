package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.PCA;
import org.apache.spark.mllib.feature.PCAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import java.util.List;

/**
 * Created by jtarraga on 30/05/17.
 */
public class PCAAnalysis implements Serializable {

    private Dataset<Row> dataset;
    private String studyId;
    private int kValue;

    private Matrix principalCompents;
    private CoordinateMatrix transposeMatrix;

    private static final int NUMBER_PC_DEFAULT = 2;

    public class PCAAnalysisResult {
        private List<String> sampleNames;
        private CoordinateMatrix inputMatrix;
        private Matrix principalComponents;

        public Matrix getProjection() throws OskarException {
            if (transposeMatrix == null && principalComponents == null) {
                throw new OskarException("Please, execute PCA before getting projection!");
            }
            return transposeMatrix.toIndexedRowMatrix().multiply(principalCompents).toBlockMatrix().toLocalMatrix();
        }

        public PCAAnalysisResult(List<String> sampleNames, CoordinateMatrix inputMatrix, Matrix principalCompents) {
            this.sampleNames = sampleNames;
            this.inputMatrix = inputMatrix;
            this.principalComponents = principalCompents;
        }

        public List<String> getSampleNames() {
            return sampleNames;
        }

        public CoordinateMatrix getInputMatrix() {
            return inputMatrix;
        }

        public Matrix getPrincipalComponents() {
            return principalComponents;
        }
    }

    public PCAAnalysisResult execute() throws OskarException {
        List<String> samples = new Oskar().metadata().samples(dataset, studyId);
        if (ListUtils.isEmpty(samples)) {
            new OskarException("Samples not found to compute PCA for study " + studyId);
        }

        JavaRDD<Vector> vecs = dataset.limit(30).toJavaRDD().map(new Function<Row, Vector>() {
            @Override
            public Vector call(Row row) throws Exception {
                double[] values = new double[samples.size()];

                GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction()
                        .apply((WrappedArray<GenericRowWithSchema>) row.apply(row.fieldIndex("studies")), studyId);
                List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
                for (int i = 0; i < samplesData.size(); i++) {
                    WrappedArray<String> sampleData = samplesData.get(i);
                    MendelianError.GenotypeCode gtCode = MendelianError
                            .getAlternateAlleleCount(new Genotype(sampleData.apply(0)));
                    double value;
                    switch (gtCode) {
                        case HET:
                            value = 1;
                            break;
                        case HOM_VAR:
                            value = 2;
                            break;
                        case HOM_REF:
                        default:
                            value = 0;
                            break;
                    }
                    values[i] = value;
                }
                return Vectors.dense(values);
            }
        });

        JavaRDD<IndexedRow> indexrows = vecs.zipWithIndex().map(new Function<Tuple2<Vector, Long>, IndexedRow>() {
            private static final long serialVersionUID = 1L;

            @Override
            public IndexedRow call(Tuple2<Vector, Long> docId) {
                return new IndexedRow(docId._2, docId._1);
            }
        });

        IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(indexrows.rdd());

        CoordinateMatrix matA = indexedRowMatrix.toCoordinateMatrix();

        transposeMatrix = matA.transpose();

        PCAModel pca = new PCA(kValue).fit(transposeMatrix.toRowMatrix().rows());
        principalCompents = pca.pc();

        PCAAnalysisResult result = new PCAAnalysisResult(samples, transposeMatrix, principalCompents);
        return result;
    }

    public PCAAnalysis(Dataset<Row> dataset, String studyId) {
        this(dataset, studyId, NUMBER_PC_DEFAULT);
    }

    public PCAAnalysis(Dataset<Row> dataset, String studyId, int kValue) {
        this.dataset = dataset;
        this.studyId = studyId;
        this.kValue = kValue;

        this.principalCompents = null;
        this.transposeMatrix = null;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public PCAAnalysis setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public PCAAnalysis setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public int getkValue() {
        return kValue;
    }

    public void setkValue(int kValue) {
        this.kValue = kValue;
    }
}
