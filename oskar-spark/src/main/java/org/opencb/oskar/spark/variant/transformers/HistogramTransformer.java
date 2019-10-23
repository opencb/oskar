package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.opencb.oskar.spark.commons.converters.DataTypeUtils;

import static org.apache.spark.sql.functions.expr;

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

    public HistogramTransformer setStep(double step) {
        set(stepParam(), step);
        return this;
    }

    public HistogramTransformer setInputCol(String inputCol) {
        set(inputColParam(), inputCol);
        return this;
    }

    public Param<Double> stepParam() {
        return stepParam == null ? stepParam = new Param<>(uid(), "step", "") : stepParam;
    }

    public Param<String> inputColParam() {
        return inputColParam == null ? inputColParam = new Param<>(uid(), "inputCol", "") : inputColParam;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        String inputCol = get(inputColParam()).get();
        Double step = get(stepParam()).get();

        DataType inputColumnType = DataTypeUtils.getField(dataset.schema(), inputCol).dataType();

        if (!(inputColumnType instanceof NumericType)) {
            throw new IllegalArgumentException("Input column must be NumericalType."
                    + " Input column '" + inputCol + "' is type " + inputColumnType.typeName());
        }

        return dataset
                .select(expr(inputCol)
                        .divide(step)
                        .cast(DataTypes.IntegerType)
                        .multiply(step)
                        .cast(inputColumnType)
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
