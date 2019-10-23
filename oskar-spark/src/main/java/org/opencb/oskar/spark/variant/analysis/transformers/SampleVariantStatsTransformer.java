package org.opencb.oskar.spark.variant.analysis.transformers;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.IndelLength;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.oskar.core.exceptions.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.types.DataTypes.*;
import static scala.collection.JavaConversions.mapAsJavaMap;
import static scala.collection.JavaConverters.asScalaIteratorConverter;

/**
 * Created on 12/09/19.
 *
 * @author Joaquin Tarraga &lt;joaquintarraga@gmail.com&gt;
 */
public class SampleVariantStatsTransformer extends AbstractTransformer implements HasStudyId  {

    private final Param<List<String>> samplesParam;
    private Param<String> fileIdParam;

    public SampleVariantStatsTransformer() {
        this(null);
    }

    public SampleVariantStatsTransformer(String uid) {
        super(uid);
        samplesParam = new Param<>(this, "samplesIds",
                "List of samplesIds");

        setDefault(samplesParam(), Collections.emptyList());
        setDefault(studyIdParam(), "");
        setDefault(fileIdParam(), "");
    }

    public static List<SampleVariantStats> toSampleVariantStats(Dataset<Row> ds) {
        List<SampleVariantStats> output = new ArrayList<>();

        for (Row row : ds.collectAsList()) {
            SampleVariantStats stats = new SampleVariantStats();

            stats.setId(row.getString(BufferUtils.SAMPLE_INDEX));
            stats.setNumVariants(row.getInt(BufferUtils.NUM_VARIANTS_INDEX));
            stats.setChromosomeCount(mapAsJavaMap(row.getMap(BufferUtils.CHROMOSOME_COUNT_INDEX)));
            stats.setTypeCount(mapAsJavaMap(row.getMap(BufferUtils.TYPE_COUNT_INDEX)));
            stats.setGenotypeCount(mapAsJavaMap(row.getMap(BufferUtils.GENOTYPE_COUNT_INDEX)));
            // Indel length
            IndelLength indel = new IndelLength(0, 0, 0, 0, 0);
            for (java.util.Map.Entry<Object, Object> entry : mapAsJavaMap(row.getMap(BufferUtils.INDEL_LENGTH_COUNT_INDEX)).entrySet()) {
                int key = (int) entry.getKey();
                int value = (int) entry.getValue();
                if (key < 5) {
                    indel.setLt5(indel.getLt5() + value);
                } else if (key < 10) {
                    indel.setLt10(indel.getLt10() + value);
                } else if (key < 15) {
                    indel.setLt15(indel.getLt15() + value);
                } else if (key < 20) {
                    indel.setLt20(indel.getLt20() + value);
                } else {
                    indel.setGte20(indel.getGte20() + value);
                }
            }
            stats.setIndelLengthCount(indel);
            stats.setNumPass(row.getInt(BufferUtils.NUM_PASS_INDEX));
            stats.setTiTvRatio((float) row.getDouble(BufferUtils.TI_TV_RATIO_INDEX));
            stats.setMeanQuality((float) row.getDouble(BufferUtils.MEAN_QUALITY_INDEX));
            stats.setStdDevQuality((float) row.getDouble(BufferUtils.STD_DEV_QUALITY_INDEX));
            stats.setMissingPositions(row.getInt(BufferUtils.MISSING_POSITIONS_INDEX));
            // TODO: mendelian errors
            stats.setConsequenceTypeCount(mapAsJavaMap(row.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNT_INDEX)));
            stats.setBiotypeCount(mapAsJavaMap(row.getMap(BufferUtils.BIOTYPE_COUNT_INDEX)));

            output.add(stats);
        }

        return output;
    }

    public Param<List<String>> samplesParam() {
        return samplesParam;
    }

    public SampleVariantStatsTransformer setSamples(List<String> samples) {
        set(samplesParam, samples);
        return this;
    }

    public SampleVariantStatsTransformer setSamples(String... samples) {
        set(samplesParam, Arrays.asList(samples));
        return this;
    }

    public List<String> getSamples() {
        return getOrDefault(samplesParam);
    }

    @Override
    public SampleVariantStatsTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public Param<String> fileIdParam() {
        return fileIdParam = fileIdParam == null
                ? new Param<>(this, "fileId", "")
                : fileIdParam;
    }

    public SampleVariantStatsTransformer setFileId(String fileId) {
        set(fileIdParam(), fileId);
        return this;
    }

    public String getFileId() {
        return getOrDefault(fileIdParam());
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        List<String> samples = getSamples();

        VariantMetadataManager metadataManager = new VariantMetadataManager();

        String studyId = getStudyId();
        if (StringUtils.isEmpty(studyId)) {
            List<String> studies = metadataManager.studies(df);
            if (studies.size() == 1) {
                studyId = studies.get(0);
            } else {
                throw OskarException.missingStudy(studies);
            }
        }
        if (samples.isEmpty()) {
            samples = metadataManager.samples(df, studyId);

            if (samples.isEmpty()) {
                throw OskarException.missingParam(samplesParam.name());
            }
        }

        List<Pedigree> pedigrees = metadataManager.pedigrees(df, studyId);

        SampleVariantStatsFunction udaf = new SampleVariantStatsFunction(studyId, getFileId(), samples, pedigrees);

        return df.agg(udaf.apply(
                col("chromosome"),
                col("reference"),
                col("alternate"),
                col("type"),
                col("length"),
                col("studies"),
                col("annotation")).alias("stats"))
                .selectExpr("stats.*")
                .withColumn("stats", explode(col("stats")))
                .selectExpr("stats.*");
    }

    private static class SampleVariantStatsFunction extends UserDefinedAggregateFunction {

        private final String studyId;
        private final String fileId;
        private final List<String> samples;
        private final Map<String, Integer> samplesPos;
        private final Map<String, Tuple2<String, String>> validChildren;

        private String sampleId;
        private Integer numVariants;
        private scala.collection.Map<String, Integer> chromosomeCount;
        private scala.collection.Map<String, Integer> typeCount;
        private scala.collection.Map<String, Integer> genotypeCount;
        private scala.collection.Map<Integer, Integer> indelLengthCount;
        private Integer numPass;
        private Integer transitions;
        private Integer transversions;
        private Integer qualityCount;
        private Double qualitySum;
        private Double qualitySumSq;
        private Integer missingPositions;
        private scala.collection.Map<Integer, Integer> mendelianErrorCount;
        private scala.collection.Map<String, Integer> consequenceTypeCount;
        private scala.collection.Map<String, Integer> biotypeCount;

        SampleVariantStatsFunction(String studyId, String fileId, List<String> samples, List<Pedigree> pedigrees) {
            this.studyId = studyId == null || studyId.isEmpty() ? null : studyId;
            this.fileId = fileId == null || fileId.isEmpty() ? null : fileId;
            //this.samplePositions = samplePositions;

            this.samples = samples;
            this.samplesPos = new LinkedHashMap<>();
            for (String sample : samples) {
                samplesPos.put(sample, samplesPos.size());
            }

            validChildren = new HashMap<String, Tuple2<String, String>>();
            if (pedigrees != null) {
                for (Pedigree pedigree : pedigrees) {
                    for (Member member: pedigree.getMembers()) {
                        if (member.getFather() != null || member.getMother() != null) {
                            String fatherId = member.getFather() == null ? null : member.getFather().getId();
                            String motherId = member.getMother() == null ? null : member.getMother().getId();
                            Tuple2<String, String> parents = new Tuple2<>(fatherId, motherId);
                            validChildren.put(member.getId(), parents);
                        }
                    }
                }
            }
        }

        @Override
        public StructType inputSchema() {
            return createStructType(new StructField[]{
                    createStructField("chromosome", StringType, false),
                    createStructField("reference", StringType, false),
                    createStructField("alternate", StringType, false),
                    createStructField("type", StringType, false),
                    createStructField("length", IntegerType, false),
                    createStructField("studies", createArrayType(VariantToRowConverter.STUDY_DATA_TYPE), false),
                    createStructField("annotation", VariantToRowConverter.ANNOTATION_DATA_TYPE, true),
                    createStructField("sampleIndices", createArrayType(IntegerType), false),
            });
        }

        @Override
        public StructType bufferSchema() {
            return BufferUtils.VARIANT_SAMPLE_STATS_BUFFER_SCHEMA;
        }

        @Override
        public DataType dataType() {
            return BufferUtils.VARIANT_SAMPLE_STATS_BUFFER_SCHEMA;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            BufferUtils.initialize(buffer, samples.size());
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            String chromosome = input.getString(0);
            String reference = input.getString(1);
            String alternate = input.getString(2);
            String type = input.getString(3);
            int length = input.getInt(4);
            Seq<Row> studies = input.getSeq(5);
            Row annotation = input.getStruct(6);

            // Sanity check (study)
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
                    throw new IllegalArgumentException("Study not found: " + studyId);
                }
            }

            List<Row> sampleStats = new ArrayList<>();

            // Loop over target samplesIds (i.e., sample indices)
            Seq<Seq<String>> samplesData = study.getSeq(study.fieldIndex("samplesData"));
            for (int i = 0; i < samples.size(); i++) {
                // Init counters
                sampleId = samples.get(i);
                initCounters(buffer, i);

                String gt = samplesData.apply(samplesPos.get(sampleId)).apply(0);

                // Missing positions
                if (gt.contains(".")) {
                    missingPositions++;
                }

                // Compute mendelian error
                Tuple2<String, String> child = validChildren.get(sampleId);
                if (child != null) {
                    Genotype childGt = new Genotype(gt);
                    Genotype fatherGt = samplesPos.get(child._1()) == null
                            ? null : new Genotype(samplesData.apply(samplesPos.get(child._1())).apply(0));
                    Genotype motherGt = samplesPos.get(child._2()) == null
                            ? null : new Genotype(samplesData.apply(samplesPos.get(child._2())).apply(0));

                    int errorCode = MendelianError.compute(fatherGt, motherGt, childGt, chromosome);
                    if (errorCode > 0) {
                        mendelianErrorCount = updated(mendelianErrorCount, errorCode);
                    }
                }

                // Only increase these counters if this sample has the mutation (i.e. has the main allele in the genotype)
                if (Genotype.hasMainAlternate(gt)) {
                    // Number of variants
                    numVariants++;

                    // Genotype count
                    genotypeCount = updated(genotypeCount, gt);

                    // Chromosome count
                    chromosomeCount = updated(chromosomeCount, chromosome);

                    // Type count
                    typeCount = updated(typeCount, type);

                    // Indel length count
                    if ("INDEL".equals(type) && length > 0) {
                        indelLengthCount = updated(indelLengthCount, length);
                    }

                    // Transition and tranversion count
                    if (VariantStats.isTransition(reference, alternate)) {
                        transitions++;
                    }
                    if (VariantStats.isTransversion(reference, alternate)) {
                        transversions++;
                    }

                    // Update file counters: quality and pass counts
                    Seq<Row> files = study.getSeq(study.fieldIndex("files"));
                    if (files != null) {
                        for (int f = 0; f < files.length(); f++) {
                            Row file = files.apply(f);
                            if (fileId != null && file.getString(file.fieldIndex("fileId")).equals(fileId)) {
                                scala.collection.Map<Object, Object> attributesMap = file.getMap(file.fieldIndex("attributes"));

                                // FILTER -> pass count
                                Option<Object> filter = attributesMap.get(StudyEntry.FILTER);
                                if (filter.isDefined() && VCFConstants.PASSES_FILTERS_v4.equals(filter.get())) {
                                    numPass++;
                                }

                                // QUAL -> quality count
                                Option<Object> qual = attributesMap.get(StudyEntry.QUAL);
                                if (qual.isDefined() && !(".").equals(qual.get())) {
                                    double qualValue = Double.valueOf((String) qual.get());
                                    qualityCount++;
                                    qualitySum += qualValue;
                                    qualitySumSq += (qualValue * qualValue);
                                }

                                break;
                            }
                        }
                    }

                    // Annotation counters
                    if (annotation != null) {
                        Set<String> biotypeSet = new HashSet<>();
                        Set<String> soSet = new HashSet<>();
                        Seq<Row> cts = annotation.getSeq(annotation.fieldIndex("consequenceTypes"));
                        if (cts != null) {
                            for (int j = 0; j < cts.length(); j++) {
                                Row ct = cts.apply(j);
                                String biotype = ct.getString(ct.fieldIndex("biotype"));
                                if (StringUtils.isNotEmpty(biotype)) {
                                    biotypeSet.add(biotype);
                                }
                                Seq<Row> sos = ct.getSeq(ct.fieldIndex("sequenceOntologyTerms"));
                                if (sos != null) {
                                    for (int k = 0; k < sos.length(); k++) {
                                        String soName = sos.apply(k).getString(1);
                                        soSet.add(soName);
                                    }
                                }
                            }
                        }

                        // Biotype count
                        for (String biotype : biotypeSet) {
                            biotypeCount = updated(biotypeCount, biotype);
                        }

                        // Consequence types count
                        for (String so : soSet) {
                            consequenceTypeCount = updated(consequenceTypeCount, so);
                        }
                    }
                }
                sampleStats.add(RowFactory.create(sampleId, numVariants, chromosomeCount, typeCount, genotypeCount, indelLengthCount,
                        numPass, transitions, transversions, 0.0d, qualityCount, 0.0d, 0.0d, 0.0d, 0.0d, missingPositions, 0.0d,
                        mendelianErrorCount, consequenceTypeCount, biotypeCount));
            }

            // Update buffer
            Seq<Row> rowSeq = asScalaIteratorConverter(sampleStats.iterator()).asScala().toSeq();
            buffer.update(0, rowSeq);
        }

        private void initCounters(MutableAggregationBuffer buffer, int i) {
            Row row = (Row) buffer.getList(0).get(i);

            numVariants = row.getInt(BufferUtils.NUM_VARIANTS_INDEX);
            chromosomeCount = row.getMap(BufferUtils.CHROMOSOME_COUNT_INDEX);
            typeCount = row.getMap(BufferUtils.TYPE_COUNT_INDEX);
            genotypeCount = row.getMap(BufferUtils.GENOTYPE_COUNT_INDEX);
            indelLengthCount = row.getMap(BufferUtils.INDEL_LENGTH_COUNT_INDEX);
            numPass = row.getInt(BufferUtils.NUM_PASS_INDEX);
            transitions = row.getInt(BufferUtils.TRANSITIONS_INDEX);
            transversions = row.getInt(BufferUtils.TRANSVERSIONS_INDEX);
            qualityCount = row.getInt(BufferUtils.QUALITY_COUNT_INDEX);
            qualitySum = row.getDouble(BufferUtils.QUALITY_SUM_INDEX);
            qualitySumSq = row.getDouble(BufferUtils.QUALITY_SUMSQ_INDEX);
            missingPositions = row.getInt(BufferUtils.MISSING_POSITIONS_INDEX);
            mendelianErrorCount = row.getMap(BufferUtils.MENDELIAN_ERROR_COUNT_INDEX);
            consequenceTypeCount = row.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNT_INDEX);
            biotypeCount = row.getMap(BufferUtils.BIOTYPE_COUNT_INDEX);
        }

        private <T> scala.collection.Map<T, Integer> updated(scala.collection.Map<T, Integer> count, T key) {
            if (!count.contains(key)) {
                return count.$plus(new Tuple2<>(key, 1));
            } else {
                return count.updated(key, count.apply(key) + 1);
            }
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            BufferUtils.merge(buffer1, buffer2, samples.size());
        }

        @Override
        public Row evaluate(Row buffer) {
            List<Row> rows = new LinkedList<>();
            for (int i = 0; i < samples.size(); i++) {
                Row bufferRow = (Row) buffer.getList(0).get(i);

                int numVariants = bufferRow.getInt(BufferUtils.NUM_VARIANTS_INDEX);
                scala.collection.Map<Object, Object> chromosomeCount = bufferRow.getMap(BufferUtils.CHROMOSOME_COUNT_INDEX);
                scala.collection.Map<Object, Object> typeCount = bufferRow.getMap(BufferUtils.TYPE_COUNT_INDEX);
                scala.collection.Map<String, Integer> genotypeCount = bufferRow.getMap(BufferUtils.GENOTYPE_COUNT_INDEX);
                scala.collection.Map<Object, Object> indelLengthCount = bufferRow.getMap(BufferUtils.INDEL_LENGTH_COUNT_INDEX);
                int numPass = bufferRow.getInt(BufferUtils.NUM_PASS_INDEX);
                int transitions = bufferRow.getInt(BufferUtils.TRANSITIONS_INDEX);
                int transversions = bufferRow.getInt(BufferUtils.TRANSVERSIONS_INDEX);
                double tiTvRatio = 1.0d * transitions / transversions;
                int qualityCount = bufferRow.getInt(BufferUtils.QUALITY_COUNT_INDEX);
                double qualitySum = bufferRow.getDouble(BufferUtils.QUALITY_SUM_INDEX);
                double qualitySumSq = bufferRow.getDouble(BufferUtils.QUALITY_SUMSQ_INDEX);
                double meanQuality = qualitySum / qualityCount;
                double stdDevQuality = Math.sqrt(qualitySumSq / qualityCount - meanQuality * meanQuality);
                int missingPositions = bufferRow.getInt(BufferUtils.MISSING_POSITIONS_INDEX);
                int numHet = 0;
                scala.collection.Iterator<Tuple2<String, Integer>> iterator = genotypeCount.toIterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Integer> entry = iterator.next();
                    if (Genotype.isHet(entry._1)) {
                        numHet += entry._2;
                    }
                }
                double heterozygosityRate = 1.0d *  numHet / numVariants;
                scala.collection.Map<Object, Object> mendelianErrorCount = bufferRow.getMap(BufferUtils.MENDELIAN_ERROR_COUNT_INDEX);
                scala.collection.Map<Object, Object> consequenceTypeCount = bufferRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNT_INDEX);
                scala.collection.Map<Object, Object> biotypeCount = bufferRow.getMap(BufferUtils.BIOTYPE_COUNT_INDEX);

                rows.add(RowFactory.create(bufferRow.getString(BufferUtils.SAMPLE_INDEX), numVariants, chromosomeCount, typeCount,
                        genotypeCount, indelLengthCount, numPass, transitions, transversions, tiTvRatio, qualityCount, qualitySum,
                        qualitySumSq, meanQuality, stdDevQuality, missingPositions, heterozygosityRate, mendelianErrorCount,
                        consequenceTypeCount, biotypeCount));
            }

            return RowFactory.create(rows);
        }
    }

    private static class BufferUtils {

        public static final String STATS_COLNAME = "stats";

        public static final String SAMPLE_COLNAME = "sample";
        public static final String NUM_VARIANTS_COLNAME = "numVariants";
        public static final String CHROMOSOME_COUNT_COLNAME = "chromosomeCount";
        public static final String TYPE_COUNT_COLNAME = "typeCount";
        public static final String GENOTYPE_COUNT_COLNAME = "genotypeCount";
        public static final String INDEL_LENGTH_COUNT_COLNAME = "indelLengthCount";
        public static final String NUM_PASS_COLNAME = "numPass";
        public static final String TRANSITIONS_COLNAME = "transitions";
        public static final String TRANSVERSIONS_COLNAME = "transversions";
        public static final String TI_TV_RATIO_COLNAME = "tiTvRatio";
        public static final String QUALITY_COUNT_COLNAME = "qualityCount";
        public static final String QUALITY_SUM_COLNAME = "qualitySum";
        public static final String QUALITY_SUMSQ_COLNAME = "qualitySumSq";
        public static final String MEAN_QUALITY_COLNAME = "meanQuality";
        public static final String STD_DEV_QUALITY_COLNAME = "stdDevQuality";
        public static final String MISSING_POSITIONS_COLNAME = "missingPositions";
        public static final String HETEROZIGOSITY_RATE_COLNAME = "heterozigosityRate";
        public static final String MENDELIAN_ERROR_COUNT_COLNAME = "mendelianErrorCount";
        public static final String CONSEQUENCE_TYPE_COUNT_COLNAME = "consequenceTypeCount";
        public static final String BIOTYPE_COUNT_COLNAME = "biotypeCount";

        public static final int SAMPLE_INDEX = 0;
        public static final int NUM_VARIANTS_INDEX = 1;
        public static final int CHROMOSOME_COUNT_INDEX = 2;
        public static final int TYPE_COUNT_INDEX = 3;
        public static final int GENOTYPE_COUNT_INDEX = 4;
        public static final int INDEL_LENGTH_COUNT_INDEX = 5;
        public static final int NUM_PASS_INDEX = 6;
        public static final int TRANSITIONS_INDEX = 7;
        public static final int TRANSVERSIONS_INDEX = 8;
        public static final int TI_TV_RATIO_INDEX = 9;
        public static final int QUALITY_COUNT_INDEX = 10;
        public static final int QUALITY_SUM_INDEX = 11;
        public static final int QUALITY_SUMSQ_INDEX = 12;
        public static final int MEAN_QUALITY_INDEX = 13;
        public static final int STD_DEV_QUALITY_INDEX = 14;
        public static final int MISSING_POSITIONS_INDEX = 15;
        public static final int HETEROZIGOSITY_RATE_INDEX = 16;
        public static final int MENDELIAN_ERROR_COUNT_INDEX = 17;
        public static final int CONSEQUENCE_TYPE_COUNT_INDEX = 18;
        public static final int BIOTYPE_COUNT_INDEX = 19;

        static final StructType SAMPLE_VARIANT_STATS_SCHEMA = createStructType(new StructField[]{
                createStructField(SAMPLE_COLNAME, StringType, false),
                createStructField(NUM_VARIANTS_COLNAME, IntegerType, false),
                createStructField(CHROMOSOME_COUNT_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(TYPE_COUNT_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(GENOTYPE_COUNT_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(INDEL_LENGTH_COUNT_COLNAME, createMapType(IntegerType, IntegerType, false), false),
                createStructField(NUM_PASS_COLNAME, IntegerType, false),
                createStructField(TRANSITIONS_COLNAME, IntegerType, false),
                createStructField(TRANSVERSIONS_COLNAME, IntegerType, false),
                createStructField(TI_TV_RATIO_COLNAME, DoubleType, false),
                createStructField(QUALITY_COUNT_COLNAME, IntegerType, false),
                createStructField(QUALITY_SUM_COLNAME, DoubleType, false),
                createStructField(QUALITY_SUMSQ_COLNAME, DoubleType, false),
                createStructField(MEAN_QUALITY_COLNAME, DoubleType, false),
                createStructField(STD_DEV_QUALITY_COLNAME, DoubleType, false),
                createStructField(MISSING_POSITIONS_COLNAME, IntegerType, false),
                createStructField(HETEROZIGOSITY_RATE_COLNAME, DoubleType, false),
                createStructField(MENDELIAN_ERROR_COUNT_COLNAME, createMapType(IntegerType, IntegerType, false), false),
                createStructField(BIOTYPE_COUNT_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(CONSEQUENCE_TYPE_COUNT_COLNAME, createMapType(StringType, IntegerType, false), false),
        });

        static final StructType VARIANT_SAMPLE_STATS_BUFFER_SCHEMA = createStructType(new StructField[]{
                createStructField(STATS_COLNAME, createArrayType(SAMPLE_VARIANT_STATS_SCHEMA, false), false),
        });

        public static void initialize(MutableAggregationBuffer buffer, int size) {
            List<Row> rows = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                rows.add(RowFactory.create("", 0, new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), 0, 0, 0, 0.0d, 0, 0.0d, 0.0d, 0.0d, 0.0d, 0, 0.0d,
                        new scala.collection.mutable.HashMap<>(), new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>()));
            }

            Seq<Row> rowSeq = asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
            buffer.update(0, rowSeq);
        }

        public static void merge(MutableAggregationBuffer buffer, Row other, int size) {
            List<Row> rows = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                Row bufferRow = (Row) buffer.getList(0).get(i);
                Row otherRow = (Row) other.getList(0).get(i);

                int numVariants = bufferRow.getInt(BufferUtils.NUM_VARIANTS_INDEX) + otherRow.getInt(BufferUtils.NUM_VARIANTS_INDEX);
                scala.collection.Map<Object, Object> chromosomeCount = bufferRow.getMap(BufferUtils.CHROMOSOME_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.CHROMOSOME_COUNT_INDEX));
                scala.collection.Map<Object, Object> typeCount = bufferRow.getMap(BufferUtils.TYPE_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.TYPE_COUNT_INDEX));
                scala.collection.Map<Object, Object> genotypeCount = bufferRow.getMap(BufferUtils.GENOTYPE_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.GENOTYPE_COUNT_INDEX));
                scala.collection.Map<Object, Object> indelLengthCount = bufferRow.getMap(BufferUtils.INDEL_LENGTH_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.INDEL_LENGTH_COUNT_INDEX));
                int numPass = bufferRow.getInt(BufferUtils.NUM_PASS_INDEX) + otherRow.getInt(BufferUtils.NUM_PASS_INDEX);
                int transitions = bufferRow.getInt(BufferUtils.TRANSITIONS_INDEX) + otherRow.getInt(BufferUtils.TRANSITIONS_INDEX);
                int transversions = bufferRow.getInt(BufferUtils.TRANSVERSIONS_INDEX) + otherRow.getInt(BufferUtils.TRANSVERSIONS_INDEX);
                int qualCount = bufferRow.getInt(BufferUtils.QUALITY_COUNT_INDEX) + otherRow.getInt(BufferUtils.QUALITY_COUNT_INDEX);
                double qualSum = bufferRow.getDouble(BufferUtils.QUALITY_SUM_INDEX) + otherRow.getDouble(BufferUtils.QUALITY_SUM_INDEX);
                double qualSumSq = bufferRow.getDouble(BufferUtils.QUALITY_SUMSQ_INDEX)
                        + otherRow.getDouble(BufferUtils.QUALITY_SUMSQ_INDEX);
                int missingPositions = bufferRow.getInt(BufferUtils.MISSING_POSITIONS_INDEX)
                        + otherRow.getInt(BufferUtils.MISSING_POSITIONS_INDEX);
                scala.collection.Map<Object, Object> mendelianErrorCount = bufferRow.getMap(BufferUtils.MENDELIAN_ERROR_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.MENDELIAN_ERROR_COUNT_INDEX));
                scala.collection.Map<Object, Object> consequenceTypeCount = bufferRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNT_INDEX));
                scala.collection.Map<Object, Object> biotypeCount = bufferRow.getMap(BufferUtils.BIOTYPE_COUNT_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.BIOTYPE_COUNT_INDEX));

                rows.add(RowFactory.create(bufferRow.getString(BufferUtils.SAMPLE_INDEX), numVariants, chromosomeCount, typeCount,
                        genotypeCount, indelLengthCount, numPass, transitions, transversions, 0.0d, qualCount, qualSum, qualSumSq, 0.0d,
                        0.0d, missingPositions, 0.0d, mendelianErrorCount, consequenceTypeCount, biotypeCount));
            }

            // Update buffer
            Seq<Row> rowSeq = asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
            buffer.update(0, rowSeq);
        }
    }
}
