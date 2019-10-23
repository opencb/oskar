package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.oskar.spark.variant.transformers.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.WrappedArray;

import java.util.*;

import static org.opencb.biodata.tools.pedigree.MendelianError.getAlternateAlleleCount;

/**
 * Created by jtarraga on 30/05/17.
 */
public class PCATransformer extends AbstractTransformer implements HasStudyId {

    private final IntParam kParam;

    private final String GENOTYPES_COLUMN_NAME = "genotypes";
    private final String GENOTYPE_COLUMN_NAME = "genotype";
    private final String ROW_COLUMN_NAME = "rowIndex";
    private final String COLUMN_COLUMN_NAME = "colIndex";
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

        Dataset<Row> ds = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row row) {
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
        }, RowEncoder.apply(createSchema(GENOTYPES_COLUMN_NAME)))
                .withColumn(ROW_COLUMN_NAME, functions.row_number().over(Window.orderBy(GENOTYPES_COLUMN_NAME)))
                .flatMap(new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterator<Row> call(Row inRow) throws Exception {
                        List<Row> output = new ArrayList<>();
                        DenseVector gts = (DenseVector) inRow.get(inRow.fieldIndex(GENOTYPES_COLUMN_NAME));
                        for (int col = 0; col < gts.size(); col++) {
                            Row outRow = RowFactory.create(
                                    inRow.getInt(inRow.fieldIndex(ROW_COLUMN_NAME)),
                                    col,
                                    gts.values()[col]);

                            output.add(outRow);
                        }
                        return output.iterator();
                    }
                }, RowEncoder.apply(createSchema3()))
                .groupByKey(new MapFunction<Row, Integer>() {
                    @Override
                    public Integer call(Row row) throws Exception {
                        // Select column index (since we are grouping by column index)
                        return row.getInt(1);
                    }
                }, Encoders.INT()).flatMapGroups(new FlatMapGroupsFunction<Integer, Row, Row>() {
                    @Override
                    public Iterator<Row> call(Integer columnIndex, Iterator<Row> iterator) throws Exception {
                        Map<Integer, Double> map = new HashMap<>();
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            // key = row index, value = GT
                            map.put(row.getInt(0), row.getDouble(2));
                        }
                        double[] values = new double[map.size()];
                        for (Integer index : map.keySet()) {
                            // Row index start by 1 (row_number() function)
                            values[index - 1] = map.get(index);
                        }

                        return Collections.singletonList(RowFactory.create(Vectors.dense(values))).iterator();
                    }
                }, RowEncoder.apply(createSchema(GENOTYPES_COLUMN_NAME)));


        // fit PCA
        PCAModel pca = new PCA()
                .setInputCol(GENOTYPES_COLUMN_NAME)
                .setOutputCol(PCA_COLUMN_NAME)
                .setK(this.getK())
                .fit(ds);

        return pca.transform(ds).select(PCA_COLUMN_NAME);
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return createSchema(PCA_COLUMN_NAME);
    }

    private StructType createSchema(String columnName) {
        return new StructType(new StructField[] {
                new StructField(columnName, new VectorUDT(), false, Metadata.empty()), });
    }

    private StructType createSchema3() {
        return new StructType(new StructField[] {
                new StructField(ROW_COLUMN_NAME, DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(COLUMN_COLUMN_NAME, DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(GENOTYPE_COLUMN_NAME, DataTypes.DoubleType, false, Metadata.empty()), });
    }
}
