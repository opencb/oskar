package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * Created on 05/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HistogramTransformer extends AbstractTransformer {

    private Param<Double> stepParam;
    private Param<String> inputColParam;

    public HistogramTransformer() {
        this(null);
    }

    public HistogramTransformer(String uid) {
        super(uid);
        setDefault(stepParam(), 0.1);
//        setDefault(inputColParam(), true);
    }

    public Param<Double> stepParam() {
        return stepParam == null ? stepParam = new Param<>(uid(), "step", "") : stepParam;
    }

    public HistogramTransformer setStep(double step) {
        set(stepParam(), step);
        return this;
    }

    public Param<String> inputColParam() {
        return inputColParam == null ? inputColParam = new Param<>(uid(), "inputCol", "") : inputColParam;
    }

    public HistogramTransformer setInputCol(String inputCol) {
        set(inputColParam(), inputCol);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String inputCol = get(inputColParam()).get();
        Double step = get(stepParam()).get();

        return dataset
                .select(col(inputCol)
                        .divide(step)
                        .cast(DataTypes.IntegerType)
                        .multiply(step)
                        .alias(inputCol))
                .groupBy(inputCol)
                .count()
                .orderBy(inputCol);
    }


    @Override
    public StructType transformSchema(StructType schema) {
        String inputCol = get(inputColParam()).get();
        StructType transformSchema = new StructType();
        for (StructField field : schema.fields()) {
            if (field.name().equals(inputCol)) {
                transformSchema.add(field);
                break;
            }
        }
        if (transformSchema.fields().length == 0) {
//            System.out.println("Input column not found! " + inputCol);
            transformSchema.add("inputCol", DataTypes.DoubleType, false);
        }
        transformSchema.add("count", DataTypes.IntegerType, false);
//        System.out.println("inputSchema ---> " + schema.json());
//        System.out.println("transformSchema --> " + transformSchema.json());
        return transformSchema;
    }

}
