package org.opencb.oskar.spark.variant.analysis.transformers;

import javafx.util.Pair;
import org.apache.commons.collections.CollectionUtils;
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
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.stats.VariantSampleStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.Option;
import scala.Tuple2;
import scala.collection.Map;
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
public class VariantSampleStatsTransformer extends AbstractTransformer implements HasStudyId  {

    private final Param<List<String>> samplesParam;
    private Param<String> fileIdParam;

    public VariantSampleStatsTransformer() {
        this(null);
    }

    public VariantSampleStatsTransformer(String uid) {
        super(uid);
        samplesParam = new Param<>(this, "samples",
                "List of samples");

        setDefault(samplesParam(), Collections.emptyList());
        setDefault(studyIdParam(), "");
        setDefault(fileIdParam(), "");
    }

    public static java.util.Map<String, VariantSampleStats> toSampleStats(Dataset<Row> ds) {
        java.util.Map<String, VariantSampleStats> output = new HashMap<>();

        for (Row row : ds.collectAsList()) {
            VariantSampleStats stats = new VariantSampleStats();

            stats.setNumVariants(row.getInt(BufferUtils.NUM_VARIANTS_INDEX));
            stats.setTiTvRatio(row.getDouble(BufferUtils.TI_TV_RATIO_INDEX));

            stats.setTypeCounter(mapAsJavaMap(row.getMap(BufferUtils.VARIANT_TYPE_COUNTS_INDEX)));
            stats.setBiotypeCounter(mapAsJavaMap(row.getMap(BufferUtils.VARIANT_BIOTYPE_COUNTS_INDEX)));
            stats.setConsequenceTypeCounter(mapAsJavaMap(row.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNTS_INDEX)));

            stats.setChromosomeCounter(mapAsJavaMap(row.getMap(BufferUtils.CHROMOSOME_COUNTS_INDEX)));

            List<Pair<String, Integer>> lofGenes = new ArrayList<>();
            for (java.util.Map.Entry<Object, Object> entry : mapAsJavaMap(row.getMap(BufferUtils.LOF_GENE_COUNTS_INDEX)).entrySet()) {
                lofGenes.add(new Pair<>((String) entry.getKey(), (Integer) entry.getValue()));
            }
            // TODO: set most mutated genes (top 50)
            stats.setMostMutatedGenes(lofGenes);

            List<Pair<String, Integer>> traits = new ArrayList<>();
            for (java.util.Map.Entry<Object, Object> entry : mapAsJavaMap(row.getMap(BufferUtils.TRAIT_COUNTS_INDEX)).entrySet()) {
                traits.add(new Pair<>((String) entry.getKey(), (Integer) entry.getValue()));
            }
            // TODO: set most frequent traits (top 50)
            stats.setMostFrequentVarTraits(traits);

            for (java.util.Map.Entry<Object, Object> entry : mapAsJavaMap(row.getMap(BufferUtils.INDEL_SIZE_COUNTS_INDEX)).entrySet()) {
                int index = ((Integer) entry.getKey()) % 5;
                if (index >= stats.getIndelLength().size()) {
                    index = stats.getIndelLength().size() - 1;
                }
                stats.getIndelLength().set(index, stats.getIndelLength().get(index) + ((Integer) entry.getValue()));
            }

            stats.setGenotypeCounter(mapAsJavaMap(row.getMap(BufferUtils.GENOTYPE_COUNTS_INDEX)));
            stats.setHeterozygosityScore(row.getDouble(BufferUtils.HETEROZIGOSITY_SCORE_INDEX));
            stats.setMissingnessScore(row.getDouble(BufferUtils.MISSINGNESS_SCORE_INDEX));

            output.put(row.getString(BufferUtils.SAMPLE_INDEX), stats);
        }

        return output;
    }

    public Param<List<String>> samplesParam() {
        return samplesParam;
    }

    public VariantSampleStatsTransformer setSamples(List<String> samples) {
        set(samplesParam, samples);
        return this;
    }

    public VariantSampleStatsTransformer setSamples(String... samples) {
        set(samplesParam, Arrays.asList(samples));
        return this;
    }

    public List<String> getSamples() {
        return getOrDefault(samplesParam);
    }

    @Override
    public VariantSampleStatsTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public Param<String> fileIdParam() {
        return fileIdParam = fileIdParam == null
                ? new Param<>(this, "fileId", "")
                : fileIdParam;
    }

    public VariantSampleStatsTransformer setFileId(String fileId) {
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

        java.util.Map.Entry<String, List<String>> entry = new VariantMetadataManager().samples(df).entrySet().iterator().next();
        String studyId = entry.getKey();
        List<String> allSamples = entry.getValue();

        List<Integer> sampleIndices = new ArrayList<>(samples.size());
        if (samples.isEmpty()) {
            samples = allSamples;
        }
        for (String sample : samples) {
            int idx = allSamples.indexOf(sample);
            if (idx < 0) {
                throw OskarException.unknownSample(studyId, sample, allSamples);
            }
            sampleIndices.add(idx);
        }

        VariantSampleStatsFunction udaf = new VariantSampleStatsFunction(getStudyId(), getFileId(), samples, sampleIndices);

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

    private static class VariantSampleStatsFunction extends UserDefinedAggregateFunction {

        private final String studyId;
        private final String fileId;
        private final List<String> samples;
        private final List<Integer> sampleIndices;

        private String sampleName;
        private Integer numVariants;
        private Integer numPass;
        private Integer transitions;
        private Integer transversions;
        private Map<String, Integer> typeCounts;
        private Map<String, Integer> biotypeCounts;
        private Map<String, Integer> consequenceTypeCounts;
        private Map<String, Integer> chromosomeCounts;
        private Map<Integer, Integer> indelSizeCounts;
        private Map<String, Integer> genotypeCounts;
        private Map<String, Integer> lofGeneCounts;
        private Map<String, Integer> traitCounts;

        VariantSampleStatsFunction(String studyId, String fileId, List<String> samples, List<Integer> sampleIndices) {
            this.studyId = studyId == null || studyId.isEmpty() ? null : studyId;
            this.fileId = fileId == null || fileId.isEmpty() ? null : fileId;
            this.samples = samples;
            this.sampleIndices = sampleIndices;
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
            BufferUtils.initialize(buffer, samples);
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

            // Loop over target samples (i.e., sample indices)
            Seq<Seq<String>> samplesData = study.getSeq(study.fieldIndex("samplesData"));
            for (int i = 0; i < sampleIndices.size(); i++) {
                // Init counters
                sampleName = samples.get(i);
                initCounters(buffer, i);

                String gt = samplesData.apply(sampleIndices.get(i)).apply(0);
                if (gt.equals("0/0") || gt.equals("0|0")) {
                    gt = "0/0";
                } else if (gt.equals("./.") || gt.equals(".|.") || gt.equals(".")) {
                    gt = "./.";
                }

                if (!genotypeCounts.contains(gt)) {
                    genotypeCounts = genotypeCounts.$plus(new Tuple2<>(gt, 1));
                } else {
                    genotypeCounts = genotypeCounts.updated(gt, genotypeCounts.apply(gt) + 1);
                }

                // If it is a variant, then update counters
                if (StringUtils.isNotEmpty(gt) && !gt.equals("./.")) {

                    // Number of variants
                    numVariants++;

                    // By chromosome counts
                    if (!chromosomeCounts.contains(chromosome)) {
                        chromosomeCounts = chromosomeCounts.$plus(new Tuple2<>(chromosome, 1));
                    } else {
                        chromosomeCounts = chromosomeCounts.updated(chromosome, chromosomeCounts.apply(chromosome) + 1);
                    }

                    // Variant type counts
                    if (!typeCounts.contains(type)) {
                        typeCounts = typeCounts.$plus(new Tuple2<>(type, 1));
                    } else {
                        typeCounts = typeCounts.updated(type, typeCounts.apply(type) + 1);
                    }

                    // By indel size counter
                    if ("INDEL".equals(type) && length > 0) {
                        if (!indelSizeCounts.contains(length)) {
                            indelSizeCounts = indelSizeCounts.$plus(new Tuple2<>(length, 1));
                        } else {
                            indelSizeCounts = indelSizeCounts.updated(length, indelSizeCounts.apply(length) + 1);
                        }
                    }

                    // Transition and tranversion counters
                    if (VariantStats.isTransition(reference, alternate)) {
                        transitions++;
                    }
                    if (VariantStats.isTransversion(reference, alternate)) {
                        transversions++;
                    }

                    // File counters
                    Seq<Row> files = study.getSeq(study.fieldIndex("files"));
                    if (files != null) {
                        for (int f = 0; f < files.length(); f++) {
                            Row file = files.apply(f);
                            if (fileId != null && !file.getString(file.fieldIndex("fileId")).equals(fileId)) {
                                continue;
                            }
                            Map<String, String> attributesMap = file.getMap(file.fieldIndex("attributes"));
                            Option<String> filter = attributesMap.get(StudyEntry.FILTER);
                            if (filter.isDefined() && filter.get().equals("PASS")) {
                                numPass++;
                            }
                        }
                    }

                    // Annotation counters
                    if (annotation != null) {
                        Set<String> biotypeSet = new HashSet<>();
                        Set<String> soSet = new HashSet<>();
                        Set<String> lofGeneSet = new HashSet<>();
                        Set<String> traitSet = new HashSet<>();
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
                                        if (VariantClassification.LOF.contains(soName)) {
                                            String ensemblGeneId = ct.getString(ct.fieldIndex("ensemblGeneId"));
                                            if (VariantClassification.LOF.contains(soName)) {
                                                if (StringUtils.isNotEmpty(ensemblGeneId)) {
                                                    lofGeneSet.add(ensemblGeneId);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Variant biotype counts
                        for (String biotype : biotypeSet) {
                            if (!biotypeCounts.contains(biotype)) {
                                biotypeCounts = biotypeCounts.$plus(new Tuple2<>(biotype, 1));
                            } else {
                                biotypeCounts = biotypeCounts.updated(biotype, biotypeCounts.apply(biotype) + 1);
                            }
                        }

                        // Consequnce types counts
                        boolean isLof = false;
                        for (String so : soSet) {
                            if (!consequenceTypeCounts.contains(so)) {
                                consequenceTypeCounts = consequenceTypeCounts.$plus(new Tuple2<>(so, 1));
                            } else {
                                consequenceTypeCounts = consequenceTypeCounts.updated(so, consequenceTypeCounts.apply(so) + 1);
                            }
                        }

                        // LoF genes
                        if (CollectionUtils.isNotEmpty(lofGeneSet)) {
                            for (String ensemblGeneId : lofGeneSet) {
                                if (!lofGeneCounts.contains(ensemblGeneId)) {
                                    lofGeneCounts = lofGeneCounts.$plus(new Tuple2<>(ensemblGeneId, 1));
                                } else {
                                    lofGeneCounts = lofGeneCounts.updated(ensemblGeneId, lofGeneCounts.apply(ensemblGeneId) + 1);
                                }
                            }
                        }

                        // Traits
                        Seq<Row> traits = annotation.getSeq(annotation.fieldIndex("traitAssociation"));
                        if (traits != null) {
                            for (int j = 0; j < traits.length(); j++) {
                                Row trait = traits.apply(j);
                                String traitId = trait.getString(trait.fieldIndex("id"));
                                if (StringUtils.isNotEmpty(traitId)) {
                                    if (!traitCounts.contains(traitId)) {
                                        traitCounts = traitCounts.$plus(new Tuple2<>(traitId, 1));
                                    } else {
                                        traitCounts = traitCounts.updated(traitId, traitCounts.apply(traitId) + 1);
                                    }
                                }
                            }
                        }
                    }
                }
                sampleStats.add(RowFactory.create(sampleName, numVariants, numPass, transitions, transversions, 0.0d, typeCounts,
                        biotypeCounts, consequenceTypeCounts, chromosomeCounts, indelSizeCounts, genotypeCounts, lofGeneCounts, traitCounts,
                        0.0d, 0.0d));
            }

            // Update buffer
            Seq<Row> rowSeq = asScalaIteratorConverter(sampleStats.iterator()).asScala().toSeq();
            buffer.update(0, rowSeq);
        }


        private void initCounters(MutableAggregationBuffer buffer, int i) {
            Row row = (Row) buffer.getList(0).get(i);

            numVariants = row.getInt(BufferUtils.NUM_VARIANTS_INDEX);
            numPass = row.getInt(BufferUtils.NUM_PASS_INDEX);
            transitions = row.getInt(BufferUtils.TRANSITIONS_INDEX);
            transversions = row.getInt(BufferUtils.TRANSVERSIONS_INDEX);
            typeCounts = row.getMap(BufferUtils.VARIANT_TYPE_COUNTS_INDEX);
            biotypeCounts = row.getMap(BufferUtils.VARIANT_BIOTYPE_COUNTS_INDEX);
            consequenceTypeCounts = row.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNTS_INDEX);
            chromosomeCounts = row.getMap(BufferUtils.CHROMOSOME_COUNTS_INDEX);
            indelSizeCounts = row.getMap(BufferUtils.INDEL_SIZE_COUNTS_INDEX);
            genotypeCounts = row.getMap(BufferUtils.GENOTYPE_COUNTS_INDEX);
            lofGeneCounts = row.getMap(BufferUtils.LOF_GENE_COUNTS_INDEX);
            traitCounts = row.getMap(BufferUtils.TRAIT_COUNTS_INDEX);
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            BufferUtils.merge(buffer1, buffer2, sampleIndices.size());
        }

        @Override
        public Row evaluate(Row buffer) {
            List<Row> rows = new LinkedList<>();
            for (int i = 0; i < samples.size(); i++) {
                Row bufferRow = (Row) buffer.getList(0).get(i);

                int numVariants = bufferRow.getInt(BufferUtils.NUM_VARIANTS_INDEX);
                int numPass = bufferRow.getInt(BufferUtils.NUM_PASS_INDEX);
                int transitions = bufferRow.getInt(BufferUtils.TRANSITIONS_INDEX);
                int transversions = bufferRow.getInt(BufferUtils.TRANSVERSIONS_INDEX);
                Map<Object, Object> typeCounts = bufferRow.getMap(BufferUtils.VARIANT_TYPE_COUNTS_INDEX);
                Map<Object, Object> biotypeCounts = bufferRow.getMap(BufferUtils.VARIANT_BIOTYPE_COUNTS_INDEX);
                Map<Object, Object> consequenceTypeCounts = bufferRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNTS_INDEX);
                Map<Object, Object> chromosomeCounts = bufferRow.getMap(BufferUtils.CHROMOSOME_COUNTS_INDEX);
                Map<Object, Object> indelSizeCounts = bufferRow.getMap(BufferUtils.INDEL_SIZE_COUNTS_INDEX);
                Map<String, Integer> genotypeCounts = bufferRow.getMap(BufferUtils.GENOTYPE_COUNTS_INDEX);
                Map<Object, Object> lofGeneCounts = bufferRow.getMap(BufferUtils.LOF_GENE_COUNTS_INDEX);
                Map<Object, Object> traitCounts = bufferRow.getMap(BufferUtils.TRAIT_COUNTS_INDEX);

                // Compute heterozigosity and missigness scores
                double heterozygosityScore;
                double missingnessScore = 0.0;

                Integer numHet = 0;
                scala.collection.Iterator<String> iterator = genotypeCounts.keys().iterator();
                while (iterator.hasNext()) {
                    String gt = iterator.next();
                    if (!"./.".equals(gt) && Genotype.isHet(gt)) {
                        Option<Integer> count = genotypeCounts.get(gt);
                        if (count.isDefined()) {
                            numHet += Integer.valueOf(count.get());
                        }
                    }
                }

                heterozygosityScore = 1.0d * numHet / numVariants;
                if (genotypeCounts.contains("./.")) {
                    Option<Integer> count = genotypeCounts.get("./.");
                    if (count.isDefined()) {
                        missingnessScore  = 1.0d * Integer.valueOf(count.get()) / (numVariants + Integer.valueOf(count.get()));
                    }
                } else {
                    missingnessScore = 0.0d;
                }

                rows.add(RowFactory.create(bufferRow.getString(BufferUtils.SAMPLE_INDEX), numVariants, numPass, transitions, transversions,
                        1.0d * transitions / transversions, typeCounts, biotypeCounts, consequenceTypeCounts, chromosomeCounts,
                        indelSizeCounts, genotypeCounts, lofGeneCounts, traitCounts, heterozygosityScore, missingnessScore));
            }

            return RowFactory.create(rows);
        }
    }

    private static class BufferUtils {

        public static final String STATS_COLNAME = "stats";

        public static final String SAMPLE_COLNAME = "sample";
        public static final String NUM_VARIANTS_COLNAME = "numVariants";
        public static final String NUM_PASS_COLNAME = "numPass";
        public static final String TRANSITIONS_COLNAME = "transitions";
        public static final String TRANSVERSIONS_COLNAME = "transversions";
        public static final String TI_TV_RATIO_COLNAME = "tiTvRatio";
        public static final String VARIANT_TYPE_COUNTS_COLNAME = "typeCounts";
        public static final String VARIANT_BIOTYPE_COUNTS_COLNAME = "biotypeCounts";
        public static final String CONSEQUENCE_TYPE_COUNTS_COLNAME = "consequenceTypeCounts";
        public static final String CHROMOSOME_COUNTS_COLNAME = "chromosomeCounts";
        public static final String INDEL_SIZE_COUNTS_COLNAME = "indelSizeCounts";
        public static final String GENOTYPE_COUNTS_COLNAME = "genotypeCounts";
        public static final String LOF_GENE_COUNTS_COLNAME = "lofGeneCounts";
        public static final String TRAIT_COUNTS_COLNAME = "traitCounts";
        public static final String HETEROZIGOSITY_SCORE_COLNAME = "heterozigosityScore";
        public static final String MISSINGNESS_SCORE_COLNAME = "missingnessScore";

        public static final int SAMPLE_INDEX = 0;
        public static final int NUM_VARIANTS_INDEX = 1;
        public static final int NUM_PASS_INDEX = 2;
        public static final int TRANSITIONS_INDEX = 3;
        public static final int TRANSVERSIONS_INDEX = 4;
        public static final int TI_TV_RATIO_INDEX = 5;
        public static final int VARIANT_TYPE_COUNTS_INDEX = 6;
        public static final int VARIANT_BIOTYPE_COUNTS_INDEX = 7;
        public static final int CONSEQUENCE_TYPE_COUNTS_INDEX = 8;
        public static final int CHROMOSOME_COUNTS_INDEX = 9;
        public static final int INDEL_SIZE_COUNTS_INDEX = 10;
        public static final int GENOTYPE_COUNTS_INDEX = 11;
        public static final int LOF_GENE_COUNTS_INDEX = 12;
        public static final int TRAIT_COUNTS_INDEX = 13;
        public static final int HETEROZIGOSITY_SCORE_INDEX = 14;
        public static final int MISSINGNESS_SCORE_INDEX = 15;

        static final StructType VARIANT_SAMPLE_STATS_SCHEMA = createStructType(new StructField[]{
                createStructField(SAMPLE_COLNAME, StringType, false),
                createStructField(NUM_VARIANTS_COLNAME, IntegerType, false),
                createStructField(NUM_PASS_COLNAME, IntegerType, false),
                createStructField(TRANSITIONS_COLNAME, IntegerType, false),
                createStructField(TRANSVERSIONS_COLNAME, IntegerType, false),
                createStructField(TI_TV_RATIO_COLNAME, DoubleType, false),
                createStructField(VARIANT_TYPE_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(VARIANT_BIOTYPE_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(CONSEQUENCE_TYPE_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(CHROMOSOME_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(INDEL_SIZE_COUNTS_COLNAME, createMapType(IntegerType, IntegerType, false), false),
                createStructField(GENOTYPE_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(LOF_GENE_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(TRAIT_COUNTS_COLNAME, createMapType(StringType, IntegerType, false), false),
                createStructField(HETEROZIGOSITY_SCORE_COLNAME, DoubleType, false),
                createStructField(MISSINGNESS_SCORE_COLNAME, DoubleType, false),
        });

        static final StructType VARIANT_SAMPLE_STATS_BUFFER_SCHEMA = createStructType(new StructField[]{
                createStructField(STATS_COLNAME, createArrayType(VARIANT_SAMPLE_STATS_SCHEMA, false), false),
        });

        public static void initialize(MutableAggregationBuffer buffer, List<String> samples) {
            List<Row> rows = new LinkedList<>();
            for (int i = 0; i < samples.size(); i++) {
                rows.add(RowFactory.create(samples.get(i), 0, 0, 0, 0, 0.0d, new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), new scala.collection.mutable.HashMap<>(),
                        new scala.collection.mutable.HashMap<>(), 0.0d, 0.0d));
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
                int numPass = bufferRow.getInt(BufferUtils.NUM_PASS_INDEX) + otherRow.getInt(BufferUtils.NUM_PASS_INDEX);
                int transitions = bufferRow.getInt(BufferUtils.TRANSITIONS_INDEX) + otherRow.getInt(BufferUtils.TRANSITIONS_INDEX);
                int transversions = bufferRow.getInt(BufferUtils.TRANSVERSIONS_INDEX) + otherRow.getInt(BufferUtils.TRANSVERSIONS_INDEX);
                Map<Object, Object> typeCounts = bufferRow.getMap(BufferUtils.VARIANT_TYPE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.VARIANT_TYPE_COUNTS_INDEX));
                Map<Object, Object> biotypeCounts = bufferRow.getMap(BufferUtils.VARIANT_BIOTYPE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.VARIANT_BIOTYPE_COUNTS_INDEX));
                Map<Object, Object> consequenceTypeCounts = bufferRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.CONSEQUENCE_TYPE_COUNTS_INDEX));
                Map<Object, Object> chromosomeCounts = bufferRow.getMap(BufferUtils.CHROMOSOME_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.CHROMOSOME_COUNTS_INDEX));
                Map<Object, Object> indelSizeCounts = bufferRow.getMap(BufferUtils.INDEL_SIZE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.INDEL_SIZE_COUNTS_INDEX));
                Map<Object, Object> genotypeCounts = bufferRow.getMap(BufferUtils.GENOTYPE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.GENOTYPE_COUNTS_INDEX));
                Map<Object, Object> lofGeneCounts = bufferRow.getMap(BufferUtils.LOF_GENE_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.LOF_GENE_COUNTS_INDEX));
                Map<Object, Object> traitCounts = bufferRow.getMap(BufferUtils.TRAIT_COUNTS_INDEX)
                        .$plus$plus(otherRow.getMap(BufferUtils.TRAIT_COUNTS_INDEX));

                rows.add(RowFactory.create(bufferRow.getString(BufferUtils.SAMPLE_INDEX), numVariants, numPass, transitions, transversions,
                        0.0d, typeCounts, biotypeCounts, consequenceTypeCounts, chromosomeCounts, indelSizeCounts, genotypeCounts,
                        lofGeneCounts, traitCounts, 0.0d, 0.0d));
            }

            // Update buffer
            Seq<Row> rowSeq = asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
            buffer.update(0, rowSeq);
        }
    }
}
