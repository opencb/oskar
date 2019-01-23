package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.WrappedArray;

import java.util.List;

import static org.opencb.biodata.tools.pedigree.MendelianError.getAlternateAlleleCount;

/**
 * Created by jtarraga on 30/05/17.
 */
public class PCATransformer extends AbstractTransformer implements HasStudyId {

    private final IntParam kParam;

    private final String GENOTYPES_COLUMN_NAME = "genotypes";
    private final String PCA_COLUMN_NAME = "PCA";

    public PCATransformer() {
        this(null);
    }

    public PCATransformer(String uid) {
        super(uid);
        kParam = new IntParam(this, "k", "");

        setDefault(kParam(), 2);
    }

    @Override
    public PCATransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public IntParam kParam() {
        return kParam;
    }

    public PCATransformer setK(int k) {
        set(kParam, k);
        return this;
    }

    public int getK() {
        return (int) getOrDefault(kParam);
    }

    // Main function
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;


        Dataset<Row> gtDataset = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction()
                        .apply((WrappedArray<GenericRowWithSchema>) row.apply(row.fieldIndex("studies")),
                                getStudyId());

                List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
                double[] values = new double[samplesData.size()];
                for (int i = 0; i < samplesData.size(); i++) {
                    WrappedArray<String> sampleData = samplesData.get(i);
                    MendelianError.GenotypeCode gtCode = getAlternateAlleleCount(new Genotype(sampleData.apply(0)));
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
                return RowFactory.create(Vectors.dense(values));
            }
        }, RowEncoder.apply(createSchema(GENOTYPES_COLUMN_NAME)));

        // fit PCA
        PCAModel pca = new PCA()
                .setInputCol(GENOTYPES_COLUMN_NAME)
                .setOutputCol(PCA_COLUMN_NAME)
                .setK(this.getK())
                .fit(gtDataset);

        return pca.transform(gtDataset).select(PCA_COLUMN_NAME);
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return createSchema(PCA_COLUMN_NAME);
    }

    public StructType createSchema(String columnName) {
        return new StructType(new StructField[] {
                new StructField(columnName, new VectorUDT(), false, Metadata.empty()), });
    }
}
