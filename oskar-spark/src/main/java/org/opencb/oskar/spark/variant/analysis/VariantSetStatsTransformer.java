package org.opencb.oskar.spark.variant.analysis;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.ChromosomeStats;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.Option;
import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static scala.collection.JavaConversions.mapAsJavaMap;

/**
 * Created on 05/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSetStatsTransformer extends AbstractTransformer implements HasStudyId  {

    private Param<String> fileIdParam;

    public VariantSetStatsTransformer() {
        this(null);
    }

    public VariantSetStatsTransformer(String studyId, String fileId) {
        super();
        if (studyId != null) {
            setStudyId(studyId);
        }
        if (fileId != null) {
            setFileId(fileId);
        }
    }

    public VariantSetStatsTransformer(String uid) {
        super(uid);
        setDefault(studyIdParam(), "");
        setDefault(fileIdParam(), "");
    }

    @Override
    public VariantSetStatsTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public Param<String> fileIdParam() {
        return fileIdParam = fileIdParam == null ? new Param<>(this, "fileId", "") : fileIdParam;
    }

    public VariantSetStatsTransformer setFileId(String fileId) {
        set(fileIdParam(), fileId);
        return this;
    }

    public String getFileId() {
        return getOrDefault(fileIdParam());
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;


        VariantSetStatsFunction udaf = new VariantSetStatsFunction(getStudyId(), getFileId());

        return df.agg(udaf.apply(
                col("chromosome"),
                col("reference"),
                col("alternate"),
                col("type"),
                col("studies"),
                col("annotation")).alias("stats")
        ).selectExpr("stats.*");
    }

    private static class VariantSetStatsFunction extends UserDefinedAggregateFunction {

        private final String studyId;
        private final String fileId;

        VariantSetStatsFunction(String studyId, String fileId) {
            this.studyId = studyId == null || studyId.isEmpty() ? null : studyId;
            this.fileId = fileId == null || fileId.isEmpty() ? null : fileId;
        }

        @Override
        public StructType inputSchema() {
            return createStructType(new StructField[]{
                    createStructField("chromosome", StringType, false),
                    createStructField("reference", StringType, false),
                    createStructField("alternate", StringType, false),
                    createStructField("type", StringType, false),
                    createStructField("studies", createArrayType(VariantToRowConverter.STUDY_DATA_TYPE), false),
                    createStructField("annotation", VariantToRowConverter.ANNOTATION_DATA_TYPE, true),
            });
        }

        @Override
        public StructType bufferSchema() {
            return VariantSetStatsBufferUtils.VARIANT_SET_BUFFER_SCHEMA;
        }

        @Override
        public DataType dataType() {
            return SchemaConverters.toSqlType(VariantSetStats.getClassSchema()).dataType();
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            VariantSetStatsBufferUtils.initialize(buffer);
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

            String chromosome = input.getString(0);
            String reference = input.getString(1);
            String alternate = input.getString(2);
            String type = input.getString(3);
            Seq<Row> studies = input.getSeq(4);
            Row annotation = input.getStruct(5);

            VariantSetStatsBufferUtils.addNumVariants(buffer, 1);
            if (VariantStats.isTransition(reference, alternate)) {
                VariantSetStatsBufferUtils.addTransitionsCount(buffer, 1);
            }
            if (VariantStats.isTransversion(reference, alternate)) {
                VariantSetStatsBufferUtils.addTransversionsCount(buffer, 1);
            }

            VariantSetStatsBufferUtils.addByChromosomeCounts(buffer, chromosome, 1);
            VariantSetStatsBufferUtils.addVariantTypeCounts(buffer, type, 1);

            updateFromStudies(buffer, studies);
            updateFromAnnotation(buffer, annotation);
        }

        private void updateFromStudies(MutableAggregationBuffer buffer, Seq<Row> studies) {
            //            Row study = studies.apply(0);
            Row study = null;
            if (StringUtils.isEmpty(studyId)) {
                if (studies.length() != 1) {
                    throw new IllegalArgumentException("Only 1 study expected. Found " + studies.length());
                }
                // Use first study
                study = studies.apply(0);
            } else {
                for (int i = 0; i < studies.length(); i++) {
                    Row thisStudy = studies.apply(i);
                    if (studyId.equals(thisStudy.getString(thisStudy.fieldIndex("studyId")))) {
                        study = thisStudy;
                    }
                }
                if (study == null) {
                    // Study not found. Nothing to do!
                    return;
                }
            }
            Seq<Row> files = study.getSeq(study.fieldIndex("files"));
            for (int i = 0; i < files.length(); i++) {
                Row file = files.apply(i);
                if (fileId != null && !file.getString(file.fieldIndex("fileId")).equals(fileId)) {
                    continue;
                }
                Map<String, String> attributesMap = file.getMap(file.fieldIndex("attributes"));
                Option<String> filter = attributesMap.get(StudyEntry.FILTER);
                if (filter.isDefined() && filter.get().equals("PASS")) {
                    VariantSetStatsBufferUtils.addNumPass(buffer, 1);
                }
                Option<String> qual = attributesMap.get(StudyEntry.QUAL);
                if (qual.isDefined() && !qual.get().isEmpty() && !qual.get().equals(".")) {
                    Double qualValue = Double.valueOf(qual.get());
                    VariantSetStatsBufferUtils.addQualCount(buffer, 1);
                    VariantSetStatsBufferUtils.addQualSum(buffer, qualValue);
                    VariantSetStatsBufferUtils.addQualSumSq(buffer, qualValue * qualValue);
                }
            }
        }

        private void updateFromAnnotation(MutableAggregationBuffer buffer, Row annotation) {
            if (annotation == null) {
                return;
            }
            Set<String> biotypeSet = new HashSet<>();
            Set<String> soSet = new HashSet<>();
            Seq<Row> cts = annotation.getSeq(annotation.fieldIndex("consequenceTypes"));
            for (int i = 0; i < cts.length(); i++) {
                Row ct = cts.apply(i);
                String biotype = ct.getString(ct.fieldIndex("biotype"));
                if (StringUtils.isNotEmpty(biotype)) {
                    biotypeSet.add(biotype);
                }
                Seq<Row> sos = ct.getSeq(ct.fieldIndex("sequenceOntologyTerms"));
                if (sos != null) {
                    for (int f = 0; f < sos.length(); f++) {
                        Row so = sos.apply(f);
                        soSet.add(so.getString(1));
                    }
                }
            }
            for (String biotype : biotypeSet) {
                VariantSetStatsBufferUtils.addVariantBiotypeCounts(buffer, biotype, 1);
            }
            for (String so : soSet) {
                VariantSetStatsBufferUtils.addConsequenceTypesCounts(buffer, so, 1);
            }
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            VariantSetStatsBufferUtils.merge(buffer1, buffer2);
        }

        @Override
        public Object evaluate(Row buffer) {
            double qualSum = VariantSetStatsBufferUtils.getQualSum(buffer);
            double qualSumSq = VariantSetStatsBufferUtils.getQualSumSq(buffer);
            double qualCount = VariantSetStatsBufferUtils.getQualCount(buffer);

            double meanQual = qualSum / qualCount;
            //Var = SumSq / n - mean * mean
            float stdDevQuality = (float) Math.sqrt(qualSumSq / qualCount - meanQual * meanQual);

            java.util.Map<String, Integer> chromosomes = mapAsJavaMap(VariantSetStatsBufferUtils.getByChromosomeCounts(buffer));
            java.util.Map<String, ChromosomeStats> chromosomeStats = new java.util.HashMap<>(chromosomes.size());
            // TODO: Calculate chromosome density
            chromosomes.forEach((chr, count) -> chromosomeStats.put(chr, new ChromosomeStats(count, 0F)));

            VariantSetStats stats = new VariantSetStats(
                    VariantSetStatsBufferUtils.getNumVariants(buffer),
                    0, // TODO: setNumSamples
                    VariantSetStatsBufferUtils.getNumPass(buffer),
                    ((float) (VariantSetStatsBufferUtils.getTransitionsCount(buffer))
                            / VariantSetStatsBufferUtils.getTransversionsCount(buffer)),
                    (float) meanQual,
                    stdDevQuality,
                    Collections.emptyList(),
                    mapAsJavaMap(VariantSetStatsBufferUtils.getVariantTypeCounts(buffer)),
                    mapAsJavaMap(VariantSetStatsBufferUtils.getVariantBiotypeCounts(buffer)),
                    mapAsJavaMap(VariantSetStatsBufferUtils.getConsequenceTypesCounts(buffer)),
                    chromosomeStats
            );
            return new VariantToRowConverter().convert(stats);
        }
    }


    private static class VariantSetStatsBufferUtils {

        static final StructType VARIANT_SET_BUFFER_SCHEMA = createStructType(new StructField[]{
                createStructField("numVariants", IntegerType, false),
                createStructField("numPass", IntegerType, false),

                createStructField("transitionsCount", IntegerType, false),
                createStructField("transversionsCount", IntegerType, false),

                createStructField("qualCount", DoubleType, false),
                createStructField("qualSum", DoubleType, false),
                createStructField("qualSumSq", DoubleType, false),

                createStructField("variantTypeCounts", createMapType(StringType, IntegerType, false), false),
                createStructField("variantBiotypeCounts", createMapType(StringType, IntegerType, false), false),
                createStructField("consequenceTypesCounts", createMapType(StringType, IntegerType, false), false),
                createStructField("byChromosomeCounts", createMapType(StringType, IntegerType, false), false),
//                createStructField("numSamples", IntegerType, false),
//                createStructField("tiTvRatio", FloatType, false),
//                createStructField("meanQuality", FloatType, false),
//                createStructField("stdDevQuality", FloatType, false),
        });


        public static void initialize(MutableAggregationBuffer buffer) {
            setNumVariants(buffer, 0);
            setNumPass(buffer, 0);
            setTransitionsCount(buffer, 0);
            setTransversionsCount(buffer, 0);
            setQualCount(buffer, 0);
            setQualSum(buffer, 0);
            setQualSumSq(buffer, 0);

            setVariantTypeCounts(buffer, new scala.collection.mutable.HashMap<>());
            setVariantBiotypeCounts(buffer, new scala.collection.mutable.HashMap<>());
            setConsequenceTypesCounts(buffer, new scala.collection.mutable.HashMap<>());
            setByChromosomeCounts(buffer, new scala.collection.mutable.HashMap<>());
        }

        public static void merge(MutableAggregationBuffer buffer, Row other) {
            addNumVariants(buffer, getNumVariants(other));
            addNumPass(buffer, getNumPass(other));
            addTransitionsCount(buffer, getTransitionsCount(other));
            addTransversionsCount(buffer, getTransversionsCount(other));
            addQualCount(buffer, getQualCount(other));
            addQualSum(buffer, getQualSum(other));
            addQualSumSq(buffer, getQualSumSq(other));

            setConsequenceTypesCounts(buffer, getConsequenceTypesCounts(buffer).$plus$plus(getConsequenceTypesCounts(other)));
            setVariantBiotypeCounts(buffer, getVariantBiotypeCounts(buffer).$plus$plus(getVariantBiotypeCounts(other)));
            setVariantTypeCounts(buffer, getVariantTypeCounts(buffer).$plus$plus(getVariantTypeCounts(other)));
            setByChromosomeCounts(buffer, getByChromosomeCounts(buffer).$plus$plus(getByChromosomeCounts(other)));
        }


        public static int getNumVariants(Row row) {
            return row.getInt(0);
        }

        public static void setNumVariants(MutableAggregationBuffer buffer, int value) {
            buffer.update(0, value);
        }

        public static void addNumVariants(MutableAggregationBuffer buffer, int value) {
            setNumVariants(buffer, getNumVariants(buffer) + value);
        }

        public static int getNumPass(Row row) {
            return row.getInt(1);
        }

        public static void setNumPass(MutableAggregationBuffer buffer, int value) {
            buffer.update(1, value);
        }

        public static void addNumPass(MutableAggregationBuffer buffer, int value) {
            setNumPass(buffer, getNumPass(buffer) + value);
        }

        public static int getTransitionsCount(Row row) {
            return row.getInt(2);
        }

        public static void setTransitionsCount(MutableAggregationBuffer buffer, int value) {
            buffer.update(2, value);
        }

        public static void addTransitionsCount(MutableAggregationBuffer buffer, int value) {
            setTransitionsCount(buffer, getTransitionsCount(buffer) + value);
        }

        public static int getTransversionsCount(Row row) {
            return row.getInt(3);
        }

        public static void setTransversionsCount(MutableAggregationBuffer buffer, int value) {
            buffer.update(3, value);
        }

        public static void addTransversionsCount(MutableAggregationBuffer buffer, int value) {
            setTransversionsCount(buffer, getTransversionsCount(buffer) + value);
        }

        public static double getQualCount(Row row) {
            return row.getDouble(4);
        }

        public static void setQualCount(MutableAggregationBuffer buffer, double value) {
            buffer.update(4, value);
        }

        public static void addQualCount(MutableAggregationBuffer buffer, double value) {
            setQualCount(buffer, getQualCount(buffer) + value);
        }

        public static double getQualSum(Row row) {
            return row.getDouble(5);
        }

        public static void setQualSum(MutableAggregationBuffer buffer, double value) {
            buffer.update(5, value);
        }

        public static void addQualSum(MutableAggregationBuffer buffer, double value) {
            setQualSum(buffer, getQualSum(buffer) + value);
        }

        public static double getQualSumSq(Row row) {
            return row.getDouble(6);
        }

        public static void setQualSumSq(MutableAggregationBuffer buffer, double value) {
            buffer.update(6, value);
        }

        public static void addQualSumSq(MutableAggregationBuffer buffer, double value) {
            setQualSumSq(buffer, getQualSumSq(buffer) + value);
        }

        public static Map<String, Integer> getVariantTypeCounts(Row row) {
            return row.getMap(7);
        }

        public static void setVariantTypeCounts(MutableAggregationBuffer buffer, Map<String, Integer> value) {
            buffer.update(7, value);
        }

        public static void addVariantTypeCounts(MutableAggregationBuffer buffer, String key, int value) {
            addToMap(buffer, key, value, 7);
        }

        public static Map<String, Integer> getVariantBiotypeCounts(Row row) {
            return row.getMap(8);
        }

        public static void setVariantBiotypeCounts(MutableAggregationBuffer buffer, Map<String, Integer> value) {
            buffer.update(8, value);
        }

        public static void addVariantBiotypeCounts(MutableAggregationBuffer buffer, String key, int value) {
            addToMap(buffer, key, value, 8);
        }

        public static Map<String, Integer> getConsequenceTypesCounts(Row row) {
            return row.getMap(9);
        }

        public static void setConsequenceTypesCounts(MutableAggregationBuffer buffer, Map<String, Integer> value) {
            buffer.update(9, value);
        }

        public static void addConsequenceTypesCounts(MutableAggregationBuffer buffer, String key, int value) {
            addToMap(buffer, key, value, 9);
        }

        public static Map<String, Integer> getByChromosomeCounts(Row row) {
            return row.getMap(10);
        }

        public static void setByChromosomeCounts(MutableAggregationBuffer buffer, Map<String, Integer> value) {
            buffer.update(10, value);
        }

        public static void addByChromosomeCounts(MutableAggregationBuffer buffer, String key, int value) {
            addToMap(buffer, key, value, 10);
        }

        private static void addToMap(MutableAggregationBuffer buffer, String key, int value, int idx) {
            Map<String, Integer> bufferMap = buffer.getMap(idx);
            Option<Integer> option = bufferMap.get(key);
            if (option.isDefined()) {
                value += option.get();
            }
            buffer.update(idx, bufferMap.$plus(new Tuple2<>(key, value)));
        }
    }
}
