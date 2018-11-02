package org.opencb.oskar.spark.variant.ml;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Calculates the Identity By State.
 *
 * Requires spark 2.1
 * https://issues.apache.org/jira/browse/SPARK-17683
 *
 * Created on 14/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IBSTransformer extends Transformer {

    private String uid;
    private Param<List<Integer>> samplesParam;
    private Param<Boolean> skipReferenceParam;
    private Param<Boolean> skipMultiAllelicParam;
    private Param<Integer> numPairsParam;

    public IBSTransformer() {
        this(null);
    }

    public IBSTransformer(String uid) {
        this.uid = uid;
        setDefault(samplesParam(), Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        setDefault(skipReferenceParam(), false);
        setDefault(skipMultiAllelicParam(), true);
        setDefault(numPairsParam(), 5);
    }

    public Param<List<Integer>> samplesParam() {
        return samplesParam = samplesParam == null ? new Param<>(this, "samples", "") : samplesParam;
    }

    public IBSTransformer setSamples(List<Integer> studyId) {
        set(samplesParam, studyId);
        return this;
    }

    public List<Integer> getSamples() {
        return getOrDefault(samplesParam);
    }

    public Param<Boolean> skipReferenceParam() {
        return skipReferenceParam = skipReferenceParam == null
                ? new Param<>(this, "skipReference", "")
                : skipReferenceParam;
    }

    public IBSTransformer setSkipReference(boolean skipReference) {
        set(skipReferenceParam, skipReference);
        return this;
    }

    public boolean getSkipReference() {
        return getOrDefault(skipReferenceParam);
    }

    public Param<Boolean> skipMultiAllelicParam() {
        return skipMultiAllelicParam = skipMultiAllelicParam == null ? new Param<>(this, "skipMultiAllelic", "") : skipMultiAllelicParam;
    }

    public IBSTransformer setSkipMultiAllelic(boolean skipMultiAllelic) {
        set(skipMultiAllelicParam, skipMultiAllelic);
        return this;
    }

    public boolean getSkipMultiAllelic() {
        return getOrDefault(skipMultiAllelicParam);
    }

    public Param<Integer> numPairsParam() {
        return numPairsParam = numPairsParam == null ? new Param<>(this, "numPairs", "") : numPairsParam;
    }

    public IBSTransformer setNumPairs(int numPairs) {
        set(numPairsParam, numPairs);
        return this;
    }

    public int getNumPairs() {
        return getOrDefault(numPairsParam);
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        int numPairs = getNumPairs();
        List<Integer> samples = getSamples();

        IdentityByStateAggregateFunction ibs = new IdentityByStateAggregateFunction(numPairs, getSkipReference(),
                getSkipMultiAllelic(), samples);

        if (true) {
            int numSamples = samples.size();
            List<String> startingPairs = new ArrayList<>();
            // First pair
            startingPairs.add(samples.get(0) + "," + samples.get(1));
            int count = 0;
            for (int i = 0; i < numSamples; i++) {
                for (int f = i + 1; f < numSamples; f++) {
                    if (count == numPairs) {
                        startingPairs.add(samples.get(i) + "," + samples.get(f));
                        count = 0;
                    }
                    count++;
                }
            }
            if (df.sparkSession().sparkContext().version().startsWith("2.0")) {
                // Lit of array not supported yet
                df = df.withColumn("startingPairs", lit(String.join("_", startingPairs)))
                        .withColumn("startingPair", explode(split(col("startingPairs"), "_")));
            } else {
                df = df.withColumn("startingPair", lit(startingPairs.toArray(new String[startingPairs.size()])))
                        .withColumn("startingPair", explode(col("startingPair")));
            }

            return df.select(col("startingPair"), col("studies").getItem(0).getField("samplesData").as("samples"))
                    .groupBy("startingPair")
                    .agg(ibs.apply(col("startingPair"), col("samples")).alias("ibs"))
                    .withColumn("ibs", explode(col("ibs")))
                    .select("ibs.*")
                    .orderBy("samplePair");
        } else if (df.sparkSession().sparkContext().version().startsWith("2.0")) {
            // Get sample pairs
            StringBuilder sb = new StringBuilder();
            int numSamples = samples.size();
            for (int i = 0; i < numSamples; i++) {
                for (int f = i + 1; f < numSamples; f++) {
                    if (sb.length() > 0) {
                        if (i % numPairs == 0) {
                            sb.append("_");
                        }
//                        else {
//                            sb.append("=");
//                        }
                    }
                    sb.append(samples.get(i)).append(",").append(samples.get(f));
                }
            }


            // lit(List) is not available in spark 2.0.x
            // See https://issues-test.apache.org/jira/browse/SPARK-17683
            Column[] columns = new Column[numPairs * 2];
            for (int i = 0; i < numPairs * 2; i++) {
                columns[i] = col("samplesPair").getItem(i).cast(DataTypes.IntegerType);
            }
            df = df.withColumn("samples", lit(sb.toString()))
                    .withColumn("samplesPair", explode(split(col("samples"), "_")))
                    .withColumn("samplesPair", split(col("samplesPair"), ","))
                    .withColumn("samplesPair", array(columns));
        } else {
            // Get sample pairs
            List<int[]> samplePairs = new ArrayList<>();
            int[] pairs = new int[numPairs * 2];
            samplePairs.add(pairs);
            int pairsCount = 0;
            int numSamples = samples.size();
            for (int i = 0; i < numSamples; i++) {
                for (int f = i + 1; f < numSamples; f++) {
                    if (pairsCount == numPairs) {
                        pairs = new int[numPairs * 2];
                        samplePairs.add(pairs);
                        pairsCount = 0;
                    }
                    pairs[pairsCount * 2] = samples.get(i);
                    pairs[pairsCount * 2 + 1] = samples.get(f);
                    pairsCount++;
                }
            }

            df = df.withColumn("samples", lit(samplePairs.toArray(new int[samplePairs.size()][])))
                    .withColumn("samplesPair", explode(col("samples")));
        }

        Column[] selectColumns = new Column[1 + numPairs * 2];
        Column[] applyColumns = new Column[numPairs * 2];
        for (int pairIdx = 0; pairIdx < numPairs; pairIdx++) {
            selectColumns[pairIdx * 2] = new Column(new UnresolvedExtractValue(
                    col("studies").getItem(0).getField("samplesData").expr(),
                    col("samplesPair").getItem(pairIdx * 2).expr())).getItem(0).alias("gt_" + pairIdx + "_1");
            selectColumns[pairIdx * 2 + 1] = new Column(new UnresolvedExtractValue(
                    col("studies").getItem(0).getField("samplesData").expr(),
                    col("samplesPair").getItem(pairIdx * 2 + 1).expr())).getItem(0).alias("gt_" + pairIdx + "_2");

            applyColumns[pairIdx * 2] = col("gt_" + pairIdx + "_1");
            applyColumns[pairIdx * 2 + 1] = col("gt_" + pairIdx + "_2");
        }
        selectColumns[numPairs * 2] = col("samplesPair");


        return df.select(selectColumns)
                .groupBy("samplesPair")
                .agg(ibs.apply(applyColumns).alias("ibs"))
                .select(col("samplesPair"), posexplode(col("ibs")))
                .withColumn("samplesPair", array(
                        new Column(new UnresolvedExtractValue(
                                col("samplesPair").expr(), col("pos").multiply(2).expr())),
                        new Column(new UnresolvedExtractValue(
                                col("samplesPair").expr(), col("pos").multiply(2).plus(1).expr()))
                ))
                .select("samplesPair", "col.*");
//        return df.select(col("samplesPair"),
//                new Column(new UnresolvedExtractValue(col("studies").getItem(0).getField("samplesData").expr(),
//                        col("samplesPair").getItem(0).expr())).getItem(0).alias("gt1"),
//                new Column(new UnresolvedExtractValue(col("studies").getItem(0).getField("samplesData").expr(),
//                        col("samplesPair").getItem(1).expr())).getItem(0).alias("gt2")
//        )
//                .groupBy("samplesPair")
//                .agg(ibs.apply(
//                        col("gt1"),
//                        col("gt2")).alias("ibs"));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return createStructType(Collections.singletonList(createStructField("ibs", DoubleType, false)));
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public String uid() {
        return getUid();
    }

    private String getUid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID("VariantStatsTransformer");
        }
        return uid;
    }

    private static class IdentityByStateAggregateFunction extends UserDefinedAggregateFunction {

        public static final int BUFFER_SCHEMA_SIZE = 5;
        public static final String DEFAULT_UNKNOWN_GENOTYPE = "./.";

        public static final Genotype MISS = new Genotype("./.");
        public static final Genotype REF = new Genotype("0/0");
        public static final Genotype HET = new Genotype("0/1");
        public static final Genotype ALT = new Genotype("1/1");
        public static final Genotype MULTI = new Genotype("1/2");

        private final int numPairs;
        private final int numSamples;
        private final List<Integer> samples;
        private final boolean skipReference;
        private final boolean skipMultiAllelic;

        IdentityByStateAggregateFunction(int numPairs, boolean skipReference,
                                         boolean skipMultiAllelic, List<Integer> samples) {
            this.numPairs = numPairs;
            this.numSamples = samples.size();
            this.samples = samples;
            this.skipReference = skipReference;
            this.skipMultiAllelic = skipMultiAllelic;
        }

//        @Override
//        public StructType inputSchema() {
//            List<StructField> fields = new ArrayList<>(2 * numPairs);
//            for (int i = 0; i < numPairs; i++) {
//                fields.add(createStructField("gt_" + i + "_1", StringType, true));
//                fields.add(createStructField("gt_" + i + "_2", StringType, true));
//            }
//            return createStructType(fields);
//        }

        @Override
        public StructType inputSchema() {
//            List<StructField> fields = new ArrayList<>(2 * numPairs);
//            for (int i = 0; i < numPairs; i++) {
//                fields.add(createStructField("startingPair", StringType, true));
//                fields.add(createStructField("samples", createArrayType(createArrayType(StringType)), true));
//            }
            return createStructType(new StructField[]{
                    createStructField("startingPair", StringType, true),
                    createStructField("samples", createArrayType(createArrayType(StringType)), true),
            });
        }

        @Override
        public StructType bufferSchema() {
            List<StructField> fields = new ArrayList<>(BUFFER_SCHEMA_SIZE * numPairs + 1);

            for (int i = 0; i < numPairs; i++) {
                fields.add(createStructField("ibs1_" + i, IntegerType, false));
                fields.add(createStructField("ibs2_" + i, IntegerType, false));
                fields.add(createStructField("ibs3_" + i, IntegerType, false));
                fields.add(createStructField("variants_" + i, IntegerType, false));
                fields.add(createStructField("skip_" + i, IntegerType, false));
            }
            fields.add(createStructField("startingPair", StringType, false));

            return createStructType(fields);
        }

        @Override
        public DataType dataType() {
//            return DoubleType;
            return createArrayType(structDataType());
        }

        private StructType structDataType() {
            return createStructType(Arrays.asList(
                    createStructField("samplePair", createArrayType(IntegerType), false),
                    createStructField("distance", DoubleType, false),
                    createStructField("counts", createArrayType(IntegerType, false), false),
                    createStructField("variants", IntegerType, false),
                    createStructField("skip", IntegerType, false)));
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            for (int i = 0; i < numPairs * BUFFER_SCHEMA_SIZE; i++) {
                buffer.update(i, 0);
            }
            buffer.update(numPairs * BUFFER_SCHEMA_SIZE, null);
        }

//        @Override
//        public void update(MutableAggregationBuffer buffer, Row input) {
//            for (int pairIdx = 0; pairIdx < numPairs; pairIdx++) {
//                updatePair(buffer, pairIdx, getGt1(input, pairIdx), getGt2(input, pairIdx));
//            }
//        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

            String startingPair = input.getString(0);
            buffer.update(numPairs * BUFFER_SCHEMA_SIZE, startingPair);

            Seq<Object> seq = (Seq<Object>) input.getSeq(1);

            iterate(startingPair, (sample1, sample2, pairIdx) -> {
                String gt1 = ((Seq<String>) seq.apply(sample1)).apply(0);
                String gt2 = ((Seq<String>) seq.apply(sample2)).apply(0);

                boolean skip = updatePair(buffer, pairIdx, gt1, gt2);
//                if (sample1 == 0 && sample2 == 2) {
//                    if (skip) {
//                        System.out.println("-- " + gt1 + " " + gt2);
//                    } else {
//                        System.out.println("++ " + gt1 + " " + gt2);
//                    }
//                }
            });
//            String startingPair = input.getString(0);
//            int idx = startingPair.indexOf(',');
//            int p1 = Integer.valueOf(startingPair.substring(0, idx));
//            int p2 = Integer.valueOf(startingPair.substring(idx + 1));
//
//            Seq<Object> seq = (Seq<Object>) input.getSeq(1);
//
//            int pairIdx = 0;
//            for (int i = p1; i < numSamples; i++) {
//                for (int f = p2 >= 0 ? p2 : i + 1; f < numSamples; f++) {
//                    p2 = -1; // invalidate p2
//
//                    int sample1 = samples.get(i);
//                    int sample2 = samples.get(f);
//
//                    String gt1 = ((Seq<String>) seq.apply(sample1)).apply(0);
//                    String gt2 = ((Seq<String>) seq.apply(sample2)).apply(0);
//
//                    updatePair(buffer, pairIdx, gt1, gt2);
//
//                    pairIdx++;
//                    if (pairIdx == numPairs) {
//                        return;
//                    }
//                    pairIdx++;
//                }
//            }

        }


        @FunctionalInterface
        interface SamplePairConsumer { void accept(int sample1, int sample2, int pairIdx); }

        public void iterate(String startingPair, SamplePairConsumer function) {
            if (startingPair == null) {
                return;
            }
            int idx = startingPair.indexOf(',');
            int p1 = Integer.valueOf(startingPair.substring(0, idx));
            int p2 = Integer.valueOf(startingPair.substring(idx + 1));

            int pairIdx = 0;
            for (int i = p1; i < numSamples; i++) {
                for (int f = p2 >= 0 ? p2 : i + 1; f < numSamples; f++) {
                    p2 = -1; // invalidate p2

                    int sample1 = samples.get(i);
                    int sample2 = samples.get(f);

                    function.accept(sample1, sample2, pairIdx);

                    pairIdx++;
                    if (pairIdx == numPairs) {
                        return;
                    }
                }
            }

        }

        private boolean updatePair(MutableAggregationBuffer buffer, int pairIdx, String gt1Str, String gt2Str) {
            IdentityByStateClustering ibsc = new IdentityByStateClustering();
            final boolean skip;
            if (gt1Str == null || gt1Str.isEmpty()
                    || gt2Str == null || gt2Str.isEmpty()) {
                skip = true;
            } else {
                if (gt1Str.equals("?/?")) {
                    gt1Str = DEFAULT_UNKNOWN_GENOTYPE;
                }
                if (gt2Str.equals("?/?")) {
                    gt2Str = DEFAULT_UNKNOWN_GENOTYPE;
                }
//                if (gt1Str.equals("./.") || gt1Str.equals(".")) {
//                    gt1Str = "0/0";
//                }
//                if (gt2Str.equals("./.") || gt2Str.equals(".")) {
//                    gt2Str = "0/0";
//                }
                if (skipReference && gt1Str.equals("0/0") && gt2Str.equals("0/0")) {
                    skip = true;
                } else {
                    Genotype gt1 = buildGenotype(gt1Str);
                    Genotype gt2 = buildGenotype(gt2Str);
                    if (gt1.getPloidy() == 2 && gt2.getPloidy() == 2
                            && ((skipMultiAllelic && gt1.getCode() == AllelesCode.ALLELES_OK && gt2.getCode() == AllelesCode.ALLELES_OK)
                            || (!skipMultiAllelic
                            && (gt1.getCode() == AllelesCode.ALLELES_OK || gt1.getCode() == AllelesCode.MULTIPLE_ALTERNATES)
                            && (gt2.getCode() == AllelesCode.ALLELES_OK || gt2.getCode() == AllelesCode.MULTIPLE_ALTERNATES)))) {
//                    if (gt1.getPloidy() == 2 && gt2.getPloidy() == 2 &&
//                            gt1.getCode() == AllelesCode.ALLELES_OK && gt2.getCode() == AllelesCode.ALLELES_OK) {

                        int sharedAlleles = ibsc.countSharedAlleles(gt1.getPloidy(), gt1, gt2);
//                    if (gt1.equals(gt2) && sharedAlleles != 2) {
//                        System.out.println("gt = " + gt1 + " shared = " + sharedAlleles);
//                    }
                        updateSharedAllelesCount(buffer, pairIdx, sharedAlleles);
                        skip = false;
                    } else {
                        skip = true;
                    }
                }
            }

            if (skip) {
                skip(buffer, pairIdx);
            } else {
                ok(buffer, pairIdx);
            }
            return skip;
        }

        private Genotype buildGenotype(String gt) {
            switch (gt) {
                case "0/0":
                case "0|0":
                    return REF;
                case "0/1":
                case "0|1":
                case "1|0":
                    return HET;
                case "1/1":
                case "1|1":
                    return ALT;
                case "1/2":
                    return MULTI;
                case "./.":
                case ".":
                    return MISS;
                default:
                    return new Genotype(gt);
            }
        }

        private void updateSharedAllelesCount(MutableAggregationBuffer buffer, int pairIdx, int sharedAlleles) {
            buffer.update(sharedAlleles + getBufferOffset(pairIdx), buffer.getInt(sharedAlleles + getBufferOffset(pairIdx)) + 1);
        }


        private static String getGt1(Row input, int pairIdx) {
            return input.getString(pairIdx * 2);
        }

        private static String getGt2(Row input, int pairIdx) {
            return input.getString(pairIdx * 2 + 1);
        }

        private static void ok(MutableAggregationBuffer buffer, int pairIdx) {
            buffer.update(3 + getBufferOffset(pairIdx), buffer.getInt(3 + getBufferOffset(pairIdx)) + 1);
        }

        private static void skip(MutableAggregationBuffer buffer, int pairIdx) {
            buffer.update(4 + getBufferOffset(pairIdx), buffer.getInt(4 + getBufferOffset(pairIdx)) + 1);
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            for (int i = 0; i < numPairs * BUFFER_SCHEMA_SIZE; i++) {
                buffer1.update(i, buffer1.getInt(i) + buffer2.getInt(i));
            }
            if (buffer1.get(numPairs * BUFFER_SCHEMA_SIZE) == null) {
                buffer1.update(numPairs * BUFFER_SCHEMA_SIZE, buffer2.get(numPairs * BUFFER_SCHEMA_SIZE));
            }
        }

        @Override
        public Object evaluate(Row buffer) {
            IdentityByStateClustering ibsc = new IdentityByStateClustering();
            IdentityByState counts = new IdentityByState();

            GenericRowWithSchema[] values = new GenericRowWithSchema[numPairs];
            String startingPair = buffer.getString(numPairs * BUFFER_SCHEMA_SIZE);

            iterate(startingPair, (sample1, sample2, pairIdx) -> {
                int offset = getBufferOffset(pairIdx);

                counts.ibs = new int[]{
                        buffer.getInt(offset),
                        buffer.getInt(offset + 1),
                        buffer.getInt(offset + 2),
                };

                double distance = ibsc.getDistance(counts);
                values[pairIdx] = new GenericRowWithSchema(new Object[]{
                        new int[]{sample1, sample2},
                        distance,
                        counts.ibs,
                        buffer.getInt(3 + offset),
                        buffer.getInt(4 + offset),
                }, structDataType());
            });
            if (values[values.length - 1] == null) {
                int idx = ArrayUtils.indexOf(values, null);
                return ArrayUtils.subarray(values, 0, idx);
            } else {
                return values;
            }

//            for (int pairIdx = 0; pairIdx < numPairs; pairIdx++) {
//                int offset = getBufferOffset(pairIdx);
//
//                counts.ibs = new int[]{
//                        buffer.getInt(offset),
//                        buffer.getInt(offset + 1),
//                        buffer.getInt(offset + 2)};
//
//                double distance = ibsc.getDistance(counts);
//                values[pairIdx] = new GenericRowWithSchema(new Object[]{
//                        distance,
//                        counts.ibs,
//                        buffer.getInt(3 + offset),
//                        buffer.getInt(4 + offset)}, structDataType());
//            }

        }

        private static int getBufferOffset(int pairIdx) {
            return pairIdx * BUFFER_SCHEMA_SIZE;
        }
    }
}
