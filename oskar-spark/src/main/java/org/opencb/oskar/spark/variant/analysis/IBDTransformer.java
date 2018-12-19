package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.IBDExpectedFrequencies;
import org.opencb.oskar.spark.variant.converters.RowToVariantConverter;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

public class IBDTransformer extends IBSTransformer {

    public IBDTransformer() {
        this(null);
    }

    public IBDTransformer(String uid) {
        super(uid);
    }

    public IBDTransformer setSamples(List<String> samples) {
        super.setSamples(samples);
        return this;
    }

    public IBDTransformer setSamples(String... samples) {
        super.setSamples(samples);
        return this;
    }

    public IBDTransformer setSkipReference(boolean skipReference) {
        super.setSkipReference(skipReference);
        return this;
    }

    public IBDTransformer setSkipMultiAllelic(boolean skipMultiAllelic) {
        super.setSkipMultiAllelic(skipMultiAllelic);
        return this;
    }

    public IBDTransformer setNumPairs(int numPairs) {
        super.setNumPairs(numPairs);
        return this;
    }

    // Main function
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        IBDExpectedFrequencies expFreqs = computeExpectedFrequencies(df);

        UserDefinedFunction z = udf(new IBDTransformer.ZFunction(expFreqs), DataTypes.createArrayType(DoubleType));
        UserDefinedFunction piHat = udf(new IBDTransformer.PiHatFunction(), DataTypes.DoubleType);

        return super.transform(df)
                .withColumn("IBD", z.apply(col("counts")))
                .withColumn("PI_HAT", piHat.apply(col("IBD")));
    }


    private IBDExpectedFrequencies computeExpectedFrequencies(Dataset<Row> df) {
        IBDExpectedFrequencies ibdExpFreqs = df.map((MapFunction<Row, IBDExpectedFrequencies>)
                        row -> {
                            IBDExpectedFrequencies freqs = new IBDExpectedFrequencies();
                            RowToVariantConverter converter = new RowToVariantConverter();
                            Variant variant = converter.convert(row);
                            freqs.update(variant);
                            return freqs;
                        },
                Encoders.bean(IBDExpectedFrequencies.class))
                .reduce((ReduceFunction<IBDExpectedFrequencies>)
                        (a, b) ->
                                new IBDExpectedFrequencies()
                                        .setE00(a.E00 + b.E00)
                                        .setE10(a.E10 + b.E10)
                                        .setE20(a.E20 + b.E20)
                                        .setE01(a.E01 + b.E01)
                                        .setE11(a.E11 + b.E11)
                                        .setE21(a.E21 + b.E21)
                                        .setE02(a.E02 + b.E02)
                                        .setE12(a.E12 + b.E12)
                                        .setE22(a.E22 + b.E22)
                                        .setCounter(a.getCounter() + b.getCounter())
                );

        ibdExpFreqs.done();

        return ibdExpFreqs;
    }


    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fds = Arrays.stream(IBSTransformer.RETURN_SCHEMA_TYPE.fields()).collect(Collectors.toList());
        fds.add(createStructField("IBD", createArrayType(DoubleType, false), false));
        fds.add(createStructField("PI_HAT", DoubleType, false));
        return createStructType(fds);
    }

    public static class ZFunction extends AbstractFunction1<WrappedArray<Integer>, WrappedArray<Double>>
            implements Serializable {

        private IBDExpectedFrequencies expFreqs;

        public ZFunction(IBDExpectedFrequencies expFreqs) {
            this.expFreqs = expFreqs;
        }

        @Override
        public WrappedArray<Double> apply(WrappedArray<Integer> ibs) {
            double s, z0, z1, z2;
            double e00, e10, e20;
            double e01, e11, e21;
            double e02, e12, e22;

            s = ibs.apply(0) + ibs.apply(1) + ibs.apply(2);

            // E_IBS[row=IBS][col=IBD]
            // E(IBD)(IBS)

            e00 = expFreqs.E00 * s;
            e10 = expFreqs.E10 * s;
            e20 = expFreqs.E20 * s;

            e01 = expFreqs.E01 * s;
            e11 = expFreqs.E11 * s;
            e21 = expFreqs.E21 * s;

            e02 = expFreqs.E02 * s;
            e12 = expFreqs.E12 * s;
            e22 = expFreqs.E22 * s;

            z0 =  ibs.apply(0) / e00;
            z1 = (ibs.apply(1) - z0 * e01) / e11;
            z2 = (ibs.apply(2) - z0 * e02 - z1 * e12) / e22;

            // Bound IBD estimates to sum to 1
            // and fall within 0-1 range
            if (z0 > 1) {
                z0 = 1;
                z1 = 0;
                z2 = 0;
            }
            if (z1 > 1) {
                z1 = 1;
                z0 = 0;
                z2 = 0;
            }
            if (z2 > 1) {
                z2 = 1;
                z0 = 0;
                z1 = 0;
            }

            if (z0 < 0) {
                s = z1 + z2;
                z1 /= s;
                z2 /= s;
                z0 = 0;
            }
            if (z1 < 0) {
                s = z0 + z2;
                z0 /= s;
                z2 /= s;
                z1 = 0;
            }
            if (z2 < 0) {
                s = z0 + z1;
                z0 /= s;
                z1 /= s;
                z2 = 0;
            }

            Double[] res = new Double[ibs.size()];
            res[0] = z0;
            res[1] = z1;
            res[2] = z2;

            return WrappedArray.<Double>make(res);
        }
    }

    public static class PiHatFunction extends AbstractFunction1<WrappedArray<Double>, Double> implements Serializable {

        @Override
        public Double apply(WrappedArray<Double> ibd) {
            // Possibly constrain IBD estimates to within possible triangle
            // i.e. 0.5 0.0 0.5 is invalid
            //
            // Constraint : z1^2 - 4 z0 z2 >= 0
            //            : x^2 - 2 pi x + z2  = 0
            //
            //              where pi = (z1 + 2 z2) / 2
            //
            // So the constaint can also be written as
            //
            //              pi^2 >=  z2

            return ibd.apply(1) / 2 + ibd.apply(2);
        }
    }
}
