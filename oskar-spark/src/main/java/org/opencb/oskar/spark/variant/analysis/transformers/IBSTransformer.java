package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.ml.param.BooleanParam;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.stats.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import scala.collection.Seq;

import java.util.*;

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
public class IBSTransformer extends AbstractTransformer {

    private final Param<List<String>> samplesParam;
    private final BooleanParam skipReferenceParam;
    private final BooleanParam skipMultiAllelicParam;
    private final IntParam numPairsParam;

    protected static final StructType RETURN_SCHEMA_TYPE = createStructType(Arrays.asList(
            createStructField("samplePair", createArrayType(StringType), false),
            createStructField("distance", DoubleType, false),
            createStructField("counts", createArrayType(IntegerType, false), false),
            createStructField("variants", IntegerType, false),
            createStructField("skip", IntegerType, false)));

    public IBSTransformer() {
        this(null);
    }

    public IBSTransformer(String uid) {
        super(uid);
        samplesParam = new Param<>(this, "samples",
                "List of samples to use for calculating the IBS");
        skipMultiAllelicParam = new BooleanParam(this, "skipMultiAllelic",
                "Skip variants where any of the samples has a secondary alternate");
        skipReferenceParam = new BooleanParam(this, "skipReference",
                "Skip variants where both samples of the pair are HOM_REF");
        numPairsParam = new IntParam(this, "numPairs", "");

        setDefault(samplesParam(), Collections.emptyList());
        setDefault(skipReferenceParam(), false);
        setDefault(skipMultiAllelicParam(), true);
        setDefault(numPairsParam(), 5);
    }

    public Param<List<String>> samplesParam() {
        return samplesParam;
    }

    public IBSTransformer setSamples(List<String> samples) {
        set(samplesParam, samples);
        return this;
    }

    public IBSTransformer setSamples(String... samples) {
        set(samplesParam, Arrays.asList(samples));
        return this;
    }

    public List<String> getSamples() {
        return getOrDefault(samplesParam);
    }

    public BooleanParam skipReferenceParam() {
        return skipReferenceParam;
    }

    public IBSTransformer setSkipReference(boolean skipReference) {
        set(skipReferenceParam, skipReference);
        return this;
    }

    public boolean getSkipReference() {
        return (boolean) getOrDefault(skipReferenceParam);
    }

    public BooleanParam skipMultiAllelicParam() {
        return skipMultiAllelicParam;
    }

    public IBSTransformer setSkipMultiAllelic(boolean skipMultiAllelic) {
        set(skipMultiAllelicParam, skipMultiAllelic);
        return this;
    }

    public boolean getSkipMultiAllelic() {
        return (boolean) getOrDefault(skipMultiAllelicParam);
    }

    public IntParam numPairsParam() {
        return numPairsParam;
    }

    public IBSTransformer setNumPairs(int numPairs) {
        set(numPairsParam, numPairs);
        return this;
    }

    public int getNumPairs() {
        return (int) getOrDefault(numPairsParam);
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        int numPairs = getNumPairs();
        List<String> samples = getSamples();
        boolean skipReference = getSkipReference();
        boolean skipMultiAllelic = getSkipMultiAllelic();

        Map.Entry<String, List<String>> entry = new VariantMetadataManager().samples(df).entrySet().iterator().next();
        String studyId = entry.getKey();
        List<String> allSamples = entry.getValue();

        List<Integer> sampleIdx = new ArrayList<>(samples.size());
        Map<Integer, String> sampleIdxMap = new HashMap<>(samples.size());
        if (samples.isEmpty()) {
            samples = allSamples;
        }
        for (String sample : samples) {
            int idx = allSamples.indexOf(sample);
            if (idx < 0) {
                throw OskarException.unknownSample(studyId, sample, allSamples);
            }
            sampleIdx.add(idx);
            sampleIdxMap.put(idx, sample);
        }

        IdentityByStateAggregateFunction ibs = new IdentityByStateAggregateFunction(numPairs, skipReference,
                skipMultiAllelic, sampleIdx, sampleIdxMap);

        int numSamples = sampleIdx.size();
        List<String> startingPairs = new ArrayList<>();
        // First pair
        startingPairs.add(sampleIdx.get(0) + "," + sampleIdx.get(1));
        int count = 0;
        for (int i = 0; i < numSamples; i++) {
            for (int f = i + 1; f < numSamples; f++) {
                if (count == numPairs) {
                    startingPairs.add(sampleIdx.get(i) + "," + sampleIdx.get(f));
                    count = 0;
                }
                count++;
            }
        }
        if (df.sparkSession().sparkContext().version().startsWith("2.0")) {
            // Lit of array not supported in Spark 2.0.x
            df = df.withColumn("startingPairs", lit(String.join("_", startingPairs)))
                    .withColumn("startingPair", explode(split(col("startingPairs"), "_")));
        } else {
            df = df.withColumn("startingPair", lit(startingPairs.toArray(new String[0])))
                    .withColumn("startingPair", explode(col("startingPair")));
        }

        return df.select(col("startingPair"), col("studies").getItem(0).getField("samplesData").as("samples"))
                .groupBy("startingPair")
                .agg(ibs.apply(col("startingPair"), col("samples")).alias("ibs"))
                .withColumn("ibs", explode(col("ibs")))
                .select("ibs.*")
                .orderBy("samplePair");

    }

    @Override
    public StructType transformSchema(StructType schema) {
        return RETURN_SCHEMA_TYPE;
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
        private final Map<Integer, String> sampleIdxMap;

        IdentityByStateAggregateFunction(int numPairs, boolean skipReference,
                                         boolean skipMultiAllelic, List<Integer> samples, Map<Integer, String> sampleIdxMap) {
            this.numPairs = numPairs;
            this.numSamples = samples.size();
            this.samples = samples;
            this.skipReference = skipReference;
            this.skipMultiAllelic = skipMultiAllelic;
            this.sampleIdxMap = sampleIdxMap;
        }

        @Override
        public StructType inputSchema() {
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
            return createArrayType(structDataType());
        }

        private StructType structDataType() {
            return RETURN_SCHEMA_TYPE;
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

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

            String startingPair = input.getString(0);
            buffer.update(numPairs * BUFFER_SCHEMA_SIZE, startingPair);

            Seq<Object> seq = (Seq<Object>) input.getSeq(1);

            iterate(startingPair, (sample1, sample2, pairIdx) -> {
                String gt1 = ((Seq<String>) seq.apply(sample1)).apply(0);
                String gt2 = ((Seq<String>) seq.apply(sample2)).apply(0);

                updatePair(buffer, pairIdx, gt1, gt2);
            });

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
            if (gt1Str == null || gt1Str.isEmpty() || gt2Str == null || gt2Str.isEmpty()) {
                skip = true;
            } else {
                Genotype gt1 = buildGenotype(gt1Str);
                Genotype gt2 = buildGenotype(gt2Str);
                if (gt1.getPloidy() != 2 || gt2.getPloidy() != 2) {
                    // Skip ploidy != 2
                    skip = true;
                } else if (anyGtMissing(gt1, gt2)) {
                    // Always skip if ANY sample is missing
                    skip = true;
                } else if (skipReference && allReference(gt1, gt2)) {
                    // If skipReference, skip if ALL are reference
                    skip = true;
                } else if (skipMultiAllelic && anyGtMiltuallelic(gt1, gt2)) {
                    // If skipMultiAllelic, skip if ANY sample is multi allelic
                    skip = true;
                } else {
                    int sharedAlleles = ibsc.countSharedAlleles(gt1.getPloidy(), gt1, gt2);
                    updateSharedAllelesCount(buffer, pairIdx, sharedAlleles);
                    skip = false;
                }
            }

            if (skip) {
                skip(buffer, pairIdx);
            } else {
                ok(buffer, pairIdx);
            }
            return skip;
        }

        private boolean allReference(Genotype gt1, Genotype gt2) {
            int[] allelesIdx = gt1.getAllelesIdx();
            int[] allelesIdx2 = gt2.getAllelesIdx();
            return allelesIdx[0] == 0 && allelesIdx[1] == 0 && allelesIdx2[0] == 0 && allelesIdx2[1] == 0;
        }

        private boolean anyGtMiltuallelic(Genotype gt1, Genotype gt2) {
            return gt1.getCode() == AllelesCode.MULTIPLE_ALTERNATES || gt2.getCode() == AllelesCode.MULTIPLE_ALTERNATES;
        }

        private boolean anyGtMissing(Genotype gt1, Genotype gt2) {
            return gt1.getCode() == AllelesCode.ALLELES_MISSING || gt2.getCode() == AllelesCode.ALLELES_MISSING;
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
                case "?/?":
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

                double distance = counts.getDistance();
                values[pairIdx] = new GenericRowWithSchema(new Object[]{
                        new String[]{sampleIdxMap.get(sample1), sampleIdxMap.get(sample2)},
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

        }

        private static int getBufferOffset(int pairIdx) {
            return pairIdx * BUFFER_SCHEMA_SIZE;
        }
    }
}
