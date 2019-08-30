package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.expr;

/**
 * Created on 29/11/18.
 *
 * @author Joaquin Tarraga Gimenez &lt;joaquintarraga@gmail.com&gt;
 */
public class ChromDensityTransformer extends AbstractTransformer {

    private Param<Integer> stepParam;
    private Param<String> chromsParam;

    public ChromDensityTransformer() {
        this(null);
    }

    public ChromDensityTransformer(String uid) {
        super(uid);
        setDefault(stepParam(), 1000000);
        setDefault(chromsParam(), "");
    }

    public ChromDensityTransformer setStep(int step) {
        set(stepParam(), step);
        return this;
    }

    public ChromDensityTransformer setChroms(String chroms) {
        set(chromsParam(), chroms);
        return this;
    }

    public Param<Integer> stepParam() {
        return stepParam == null ? stepParam = new Param<>(uid(), "step", "") : stepParam;
    }

    public Param<String> chromsParam() {
        return chromsParam == null ? chromsParam = new Param<>(uid(), "chroms", "") : chromsParam;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> res = (Dataset<Row>) dataset;

        String chromosomes = getOrDefault(chromsParam());
        Integer step = getOrDefault(stepParam());

        if (StringUtils.isNotEmpty(chromosomes)) {
            String[] chroms = chromosomes.split(",");
            StringBuilder include = new StringBuilder("chromosome='").append(chroms[0]).append("'");
            for (int i = 1; i < chroms.length; i++) {
                include.append(" OR chromosome='").append(chroms[i]).append("'");
            }
            res = res.filter(include.toString());
        }

        return res.select(expr("chromosome"),
                expr("start")
                        .divide(step)
                        .cast(DataTypes.IntegerType)
                        .multiply(step)
                        .cast(DataTypes.IntegerType)
                        .alias("position"))
                .groupBy("chromosome", "position")
                .count()
                .orderBy("chromosome", "position");
    }


    @Override
    public StructType transformSchema(StructType schema) {
        StructType transformSchema = new StructType();
        transformSchema.add("chromosome", DataTypes.StringType, false);
        transformSchema.add("position", DataTypes.IntegerType, false);
        transformSchema.add("count", DataTypes.IntegerType, false);
        return transformSchema;
    }
}
