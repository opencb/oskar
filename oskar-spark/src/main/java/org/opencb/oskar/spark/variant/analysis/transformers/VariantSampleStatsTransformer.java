package org.opencb.oskar.spark.variant.analysis.transformers;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
import org.opencb.commons.utils.CollectionUtils;
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
import static org.apache.spark.sql.types.DataTypes.*;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.mapAsJavaMap;

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

//    public VariantSampleStatsTransformer(String studyId, String fileId) {
//        super();
//        if (studyId != null) {
//            setStudyId(studyId);
//        }
//        if (fileId != null) {
//            setFileId(fileId);
//        }
//
//        sampleIndices = new Integer[3];
//        sampleIndices[0] = 1;
//        sampleIndices[1] = 3;
//        sampleIndices[2] = 5;
//    }

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

        List<Row> rows = ds.collectAsList();
        Row row = rows.get(0);

        Seq<String> samples = VariantSampleStatsBufferUtils.getSamples(row);
        for (int i = 0; i < samples.size(); i++) {
            VariantSampleStats stats = new VariantSampleStats();

            stats.setNumVariants(VariantSampleStatsBufferUtils.getNumVariants(row).apply(i));
            stats.setTiTvRatio(1.0D * VariantSampleStatsBufferUtils.getTransitions(row).apply(i)
                    / VariantSampleStatsBufferUtils.getTransversions(row).apply(i));

            stats.setTypeCounter(mapAsJavaMap(VariantSampleStatsBufferUtils.getVariantTypeCounts(row).apply(i)));
            stats.setBiotypeCounter(mapAsJavaMap(VariantSampleStatsBufferUtils.getVariantBiotypeCounts(row).apply(i)));
            stats.setConsequenceTypeCounter(mapAsJavaMap(VariantSampleStatsBufferUtils.getConsequenceTypeCounts(row).apply(i)));

            stats.setChromosomeCounter(mapAsJavaMap(VariantSampleStatsBufferUtils.getByChromosomeCounts(row).apply(i)));

            List<Pair<String, Integer>> lofGenes = new ArrayList<>();
            for (java.util.Map.Entry<String, Integer> entry : mapAsJavaMap(VariantSampleStatsBufferUtils.getLofGeneCounts(row).apply(i))
                    .entrySet()) {
                lofGenes.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
            stats.setMostMutatedGenes(lofGenes);

            List<Pair<String, Integer>> traits = new ArrayList<>();
            for (java.util.Map.Entry<String, Integer> entry : mapAsJavaMap(VariantSampleStatsBufferUtils.getTraitCounts(row).apply(i))
                    .entrySet()) {
                traits.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
            stats.setMostFrequentVarTraits(traits);

            Map<Integer, Integer> indelSizeMap = VariantSampleStatsBufferUtils.getByIndelSizeCounts(row).apply(i);
            scala.collection.Iterator<Tuple2<Integer, Integer>> iterator = indelSizeMap.iterator();
            while (iterator.hasNext()) {
                Tuple2<Integer, Integer> entry = iterator.next();
                int index = entry._1() % 5;
                if (index >= stats.getIndelLength().size()) {
                    index = stats.getIndelLength().size() - 1;
                }
                stats.getIndelLength().set(index, stats.getIndelLength().get(index) + entry._2());
            }

            stats.setGenotypeCounter(mapAsJavaMap(VariantSampleStatsBufferUtils.getGenotypeCounts(row).apply(i)));

            int numHet = 0;
            for (String gt: stats.getGenotypeCounter().keySet()) {
                if (!"./.".equals(gt) && Genotype.isHet(gt)) {
                    numHet += stats.getGenotypeCounter().get(gt);
                }
            }

            // Set most affected genes (top 50)
//            stats.setMostMutatedGenes(getTop50(geneCounters.get(i)));

            // Set most frequent variant traits (top 50)
//            stats.setMostFrequentVarTraits(getTop50(varTraitCounters.get(i)));

            // Compute heterozigosity and missigness scores
            stats.setHeterozygosityScore(1.0D * numHet / stats.getNumVariants());
            if (stats.getGenotypeCounter().containsKey("./.")) {
                stats.setMissingnessScore(1.0D * stats.getGenotypeCounter().get("./.")
                        / (stats.getNumVariants() + stats.getGenotypeCounter().get("./.")));
            } else {
                stats.setMissingnessScore(0.0D);
            }

            output.put(samples.apply(i), stats);
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
        return fileIdParam = fileIdParam == null ? new Param<>(this, "fileId", "") : fileIdParam;
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
                col("annotation")).alias("stats")).selectExpr("stats.*");
    }

    private static class VariantSampleStatsFunction extends UserDefinedAggregateFunction {

        private final String studyId;
        private final String fileId;
        private final List<String> samples;
        private final List<Integer> sampleIndices;

        private List<Integer> numVariants = new LinkedList<>();
        private List<Integer> numPass = new LinkedList<>();
        private List<Integer> transitions = new LinkedList<>();
        private List<Integer> transversions = new LinkedList<>();
        private List<Map<String, Integer>> genotypeCounts = new LinkedList<>();
        private List<Map<String, Integer>> variantTypeCounts = new LinkedList<>();
        private List<Map<String, Integer>> variantBiotypeCounts = new LinkedList<>();
        private List<Map<String, Integer>> consequenceTypeCounts = new LinkedList<>();
        private List<Map<String, Integer>> byChromosomeCounts = new LinkedList<>();
        private List<Map<Integer, Integer>> byIndelSizeCounts = new LinkedList<>();
        private List<Map<String, Integer>> lofGeneCounts = new LinkedList<>();
        private List<Map<String, Integer>> traitCounts = new LinkedList<>();

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
            return VariantSampleStatsBufferUtils.VARIANT_SAMPLE_STATS_BUFFER_SCHEMA;
        }

        @Override
        public DataType dataType() {
            return VariantSampleStatsBufferUtils.VARIANT_SAMPLE_STATS_BUFFER_SCHEMA;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            VariantSampleStatsBufferUtils.initialize(buffer, samples);
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

            // Init counters
            preUpdate();

            // Loop over target samples (i.e., sample indices)
            Seq<Seq<String>> samplesData = study.getSeq(study.fieldIndex("samplesData"));
            for (int i = 0; i < sampleIndices.size(); i++) {
                String gt = samplesData.apply(sampleIndices.get(i)).apply(0);
                if (gt.equals("0/0") || gt.equals("0|0")) {
                    gt = "0/0";
                } else if (gt.equals("./.") || gt.equals(".|.") || gt.equals(".")) {
                    gt = "./.";
                }

                genotypeCounts.add(VariantSampleStatsBufferUtils.getGenotypeCounts(buffer).apply(i));
                if (!genotypeCounts.get(i).contains(gt)) {
                    genotypeCounts.set(i, genotypeCounts.get(i).$plus(new Tuple2<>(gt, 1)));
                } else {
                    genotypeCounts.set(i, genotypeCounts.get(i)
                            .updated(gt, genotypeCounts.get(i).apply(gt) + 1));
                }

                numVariants.add(VariantSampleStatsBufferUtils.getNumVariants(buffer).apply(i));
                numPass.add(VariantSampleStatsBufferUtils.getNumPass(buffer).apply(i));
                transitions.add(VariantSampleStatsBufferUtils.getTransitions(buffer).apply(i));
                transversions.add(VariantSampleStatsBufferUtils.getTransversions(buffer).apply(i));
                variantTypeCounts.add(VariantSampleStatsBufferUtils.getVariantTypeCounts(buffer).apply(i));
                variantBiotypeCounts.add(VariantSampleStatsBufferUtils.getVariantBiotypeCounts(buffer).apply(i));
                consequenceTypeCounts.add(VariantSampleStatsBufferUtils.getConsequenceTypeCounts(buffer).apply(i));
                byChromosomeCounts.add(VariantSampleStatsBufferUtils.getByChromosomeCounts(buffer).apply(i));
                byIndelSizeCounts.add(VariantSampleStatsBufferUtils.getByIndelSizeCounts(buffer).apply(i));
                lofGeneCounts.add(VariantSampleStatsBufferUtils.getLofGeneCounts(buffer).apply(i));
                traitCounts.add(VariantSampleStatsBufferUtils.getTraitCounts(buffer).apply(i));

                // If it is a variant, then update counters
                if (StringUtils.isNotEmpty(gt) && !gt.equals("./.")) {

                    // Number of variants
                    numVariants.set(i, numVariants.get(i) + 1);

                    // By chromosome counts
                    if (!byChromosomeCounts.get(i).contains(chromosome)) {
                        byChromosomeCounts.set(i, byChromosomeCounts.get(i).$plus(new Tuple2<>(chromosome, 1)));
                    } else {
                        byChromosomeCounts.set(i, byChromosomeCounts.get(i)
                                .updated(chromosome, byChromosomeCounts.get(i).apply(chromosome) + 1));
                    }

                    // Variant type counts
                    if (!variantTypeCounts.get(i).contains(type)) {
                        variantTypeCounts.set(i, variantTypeCounts.get(i).$plus(new Tuple2<>(type, 1)));
                    } else {
                        variantTypeCounts.set(i, variantTypeCounts.get(i).updated(type, variantTypeCounts.get(i).apply(type) + 1));
                    }

                    // By indel size counter
                    if ("INDEL".equals(type) && length > 0) {
                        if (!byIndelSizeCounts.get(i).contains(length)) {
                            byIndelSizeCounts.set(i, byIndelSizeCounts.get(i).$plus(new Tuple2<>(length, 1)));
                        } else {
                            byIndelSizeCounts.set(i, byIndelSizeCounts.get(i).updated(length, byIndelSizeCounts.get(i).apply(length) + 1));
                        }
                    }

                    // Transition and tranversion counters
                    if (VariantStats.isTransition(reference, alternate)) {
                        transitions.set(i, transitions.get(i) + 1);
                    }
                    if (VariantStats.isTransversion(reference, alternate)) {
                        transversions.set(i, transversions.get(i) + 1);
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
                                numPass.set(i, numPass.get(i) + 1);
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
                            if (!variantBiotypeCounts.get(i).contains(biotype)) {
                                variantBiotypeCounts.set(i, variantBiotypeCounts.get(i).$plus(new Tuple2<>(biotype, 1)));
                            } else {
                                variantBiotypeCounts.set(i, variantBiotypeCounts.get(i)
                                        .updated(biotype, variantBiotypeCounts.get(i).apply(biotype) + 1));
                            }
                        }

                        // Consequnce types counts
                        boolean isLof = false;
                        for (String so : soSet) {
                            if (!consequenceTypeCounts.get(i).contains(so)) {
                                consequenceTypeCounts.set(i, consequenceTypeCounts.get(i).$plus(new Tuple2<>(so, 1)));
                            } else {
                                consequenceTypeCounts.set(i, consequenceTypeCounts.get(i)
                                        .updated(so, consequenceTypeCounts.get(i).apply(so) + 1));
                            }
                        }

                        // LoF genes
                        if (CollectionUtils.isNotEmpty(lofGeneSet)) {
                            for (String ensemblGeneId : lofGeneSet) {
                                if (!lofGeneCounts.get(i).contains(ensemblGeneId)) {
                                    lofGeneCounts.set(i, lofGeneCounts.get(i).$plus(new Tuple2<>(ensemblGeneId, 1)));
                                } else {
                                    lofGeneCounts.set(i, lofGeneCounts.get(i)
                                            .updated(ensemblGeneId, lofGeneCounts.get(i).apply(ensemblGeneId) + 1));
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
                                    traitSet.add(traitId);
                                }
                            }
                        }
                    }
                }
            }

            // Initialize buffer
            postUpdate(buffer);
        }


        private void preUpdate() {
            // Init counters
            numVariants = new LinkedList<>();
            numPass = new LinkedList<>();
            transitions = new LinkedList<>();
            transversions = new LinkedList<>();
            genotypeCounts = new LinkedList<>();
            variantTypeCounts = new LinkedList<>();
            variantBiotypeCounts = new LinkedList<>();
            consequenceTypeCounts = new LinkedList<>();
            byChromosomeCounts = new LinkedList<>();
            byIndelSizeCounts = new LinkedList<>();
            lofGeneCounts = new LinkedList<>();
            traitCounts = new LinkedList<>();
        }

        private void postUpdate(MutableAggregationBuffer buffer) {
            VariantSampleStatsBufferUtils.setGenotypeCounts(buffer, asScalaBuffer(genotypeCounts).toSeq());
            VariantSampleStatsBufferUtils.setNumVariants(buffer, asScalaBuffer(numVariants).toSeq());
            VariantSampleStatsBufferUtils.setNumPass(buffer, asScalaBuffer(numPass).toSeq());
            VariantSampleStatsBufferUtils.setTransitions(buffer, asScalaBuffer(transitions).toSeq());
            VariantSampleStatsBufferUtils.setTransversions(buffer, asScalaBuffer(transversions).toSeq());

            VariantSampleStatsBufferUtils.setVariantTypeCounts(buffer, asScalaBuffer(variantTypeCounts).toSeq());
            VariantSampleStatsBufferUtils.setVariantBiotypeCounts(buffer, asScalaBuffer(variantBiotypeCounts).toSeq());
            VariantSampleStatsBufferUtils.setConsequenceTypeCounts(buffer, asScalaBuffer(consequenceTypeCounts).toSeq());
            VariantSampleStatsBufferUtils.setByChromosomeCounts(buffer, asScalaBuffer(byChromosomeCounts).toSeq());
            VariantSampleStatsBufferUtils.setByIndelSizeCounts(buffer, asScalaBuffer(byIndelSizeCounts).toSeq());
            VariantSampleStatsBufferUtils.setLofGeneCounts(buffer, asScalaBuffer(lofGeneCounts).toSeq());
            VariantSampleStatsBufferUtils.setTraitCounts(buffer, asScalaBuffer(traitCounts).toSeq());
        }

        private void updateFromStudies(MutableAggregationBuffer buffer, Seq<Row> studies) {
//            Row study = null;
//            if (StringUtils.isEmpty(studyId)) {
//                if (studies.length() != 1) {
//                    throw new IllegalArgumentException("Only 1 study expected. Found " + studies.length());
//                }
//                // Use first study
//                study = studies.apply(0);
//            } else {
//                for (int i = 0; i < studies.length(); i++) {
//                    Row thisStudy = studies.apply(i);
//                    if (studyId.equals(thisStudy.getString(thisStudy.fieldIndex("studyId")))) {
//                        study = thisStudy;
//                    }
//                }
//                if (study == null) {
//                    // Study not found. Nothing to do!
//                    return;
//                }
//            }
//            Seq<Row> files = study.getSeq(study.fieldIndex("files"));
//            for (int i = 0; i < files.length(); i++) {
//                Row file = files.apply(i);
//                if (fileId != null && !file.getString(file.fieldIndex("fileId")).equals(fileId)) {
//                    continue;
//                }
//                Map<String, String> attributesMap = file.getMap(file.fieldIndex("attributes"));
//                Option<String> filter = attributesMap.get(StudyEntry.FILTER);
//                if (filter.isDefined() && filter.get().equals("PASS")) {
//                    VariantSampleStatsBufferUtils.addNumPass(buffer, 1);
//                }
//                Option<String> qual = attributesMap.get(StudyEntry.QUAL);
//                if (qual.isDefined() && !qual.get().isEmpty() && !qual.get().equals(".")) {
//                    Double qualValue = Double.valueOf(qual.get());
//                    VariantSampleStatsBufferUtils.addQualCount(buffer, 1);
//                    VariantSampleStatsBufferUtils.addQualSum(buffer, qualValue);
//                    VariantSampleStatsBufferUtils.addQualSumSq(buffer, qualValue * qualValue);
//                }
//            }
        }

        private void updateFromAnnotation(MutableAggregationBuffer buffer, Row annotation) {
//            if (annotation == null) {
//                return;
//            }
//            Set<String> biotypeSet = new HashSet<>();
//            Set<String> soSet = new HashSet<>();
//            Seq<Row> cts = annotation.getSeq(annotation.fieldIndex("consequenceTypes"));
//            for (int i = 0; i < cts.length(); i++) {
//                Row ct = cts.apply(i);
//                String biotype = ct.getString(ct.fieldIndex("biotype"));
//                if (StringUtils.isNotEmpty(biotype)) {
//                    biotypeSet.add(biotype);
//                }
//                Seq<Row> sos = ct.getSeq(ct.fieldIndex("sequenceOntologyTerms"));
//                if (sos != null) {
//                    for (int f = 0; f < sos.length(); f++) {
//                        Row so = sos.apply(f);
//                        soSet.add(so.getString(1));
//                    }
//                }
//            }
//            for (String biotype : biotypeSet) {
//                VariantSampleStatsBufferUtils.addVariantBiotypeCounts(buffer, biotype, 1);
//            }
//            for (String so : soSet) {
//                VariantSampleStatsBufferUtils.addConsequenceTypesCounts(buffer, so, 1);
//            }
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            VariantSampleStatsBufferUtils.merge(buffer1, buffer2, sampleIndices.size());
        }

        @Override
        public Object evaluate(Row buffer) {
            return buffer;

//            Seq<String> samples = VariantSampleStatsBufferUtils.getSamples(buffer);
//            Seq<Integer> numVariants = VariantSampleStatsBufferUtils.getNumVariants(buffer);
//
//            Map<String, VariantSetStats> output = new scala.collection.mutable.HashMap<>();
//            for (int i = 0; i < samples.size(); i++) {
//                VariantSetStats stats = new VariantSetStats();
//                stats.setNumVariants(numVariants.apply(i));
//
//                ((scala.collection.mutable.HashMap<String, VariantSetStats>) output).put(samples.apply(i), stats);
//            }
//
//            return new VariantToRowConverter().convert(output, VariantSampleStatsBufferUtils.VARIANT_SAMPLE_STATS_OUTPUT_SCHEMA);

//            double qualSum = VariantSampleStatsBufferUtils.getQualSum(buffer);
//            double qualSumSq = VariantSampleStatsBufferUtils.getQualSumSq(buffer);
//            double qualCount = VariantSampleStatsBufferUtils.getQualCount(buffer);
//
//            double meanQual = qualSum / qualCount;
//            //Var = SumSq / n - mean * mean
//            float stdDevQuality = (float) Math.sqrt(qualSumSq / qualCount - meanQual * meanQual);
//
//            java.util.Map<String, Integer> chromosomes = mapAsJavaMap(VariantSampleStatsBufferUtils.getByChromosomeCounts(buffer));
//            java.util.Map<String, ChromosomeStats> chromosomeStats = new java.util.HashMap<>(chromosomes.size());
//            // TODO: Calculate chromosome density
//            chromosomes.forEach((chr, count) -> chromosomeStats.put(chr, new ChromosomeStats(count, 0F)));
//
//            VariantSetStats stats = new VariantSetStats(
//                    VariantSampleStatsBufferUtils.getNumVariants(buffer),
//                    0, // TODO: setNumSamples
//                    VariantSampleStatsBufferUtils.getNumPass(buffer),
//                    ((float) (VariantSampleStatsBufferUtils.getTransitionsCount(buffer))
//                            / VariantSampleStatsBufferUtils.getTransversionsCount(buffer)),
//                    (float) meanQual,
//                    stdDevQuality,
//                    Collections.emptyList(),
//                    mapAsJavaMap(VariantSampleStatsBufferUtils.getVariantTypeCounts(buffer)),
//                    mapAsJavaMap(VariantSampleStatsBufferUtils.getVariantBiotypeCounts(buffer)),
//                    mapAsJavaMap(VariantSampleStatsBufferUtils.getConsequenceTypesCounts(buffer)),
//                    chromosomeStats
//            );
//            return new VariantToRowConverter().convert(stats);
        }
    }


    private static class VariantSampleStatsBufferUtils {

        public static final String SAMPLES_COLNAME = "samples";
        public static final String NUM_VARIANTS_COLNAME = "numVariants";
        public static final String VARIANT_TYPE_COUNTS_COLNAME = "variantTypeCounts";
        public static final String NUM_PASS_COLNAME = "numPass";
        public static final String TRANSITIONS_COLNAME = "transitions";
        public static final String TRANSVERSIONS_COLNAME = "transversions";
        public static final String VARIANT_BIOTYPE_COUNTS_COLNAME = "variantBiotypeCounts";
        public static final String CONSEQUENCE_TYPE_COUNTS_COLNAME = "consequenceTypeCounts";
        public static final String CHROMOSOME_COUNTS_COLNAME = "byChromosomeCounts";
        public static final String INDEL_SIZE_COUNTS_COLNAME = "byIndelSizeCounts";
        public static final String GENOTYPE_COUNTS_COLNAME = "genotypeCounts";
        public static final String LOF_GENE_COUNTS_COLNAME = "lofGeneCounts";
        public static final String TRAIT_COUNTS_COLNAME = "traitCounts";

        static final StructType VARIANT_SAMPLE_STATS_BUFFER_SCHEMA = createStructType(new StructField[]{
                createStructField(SAMPLES_COLNAME, createArrayType(StringType), false),
                createStructField(NUM_VARIANTS_COLNAME, createArrayType(IntegerType), false),
                createStructField(NUM_PASS_COLNAME, createArrayType(IntegerType), false),
                createStructField(TRANSITIONS_COLNAME, createArrayType(IntegerType), false),
                createStructField(TRANSVERSIONS_COLNAME, createArrayType(IntegerType), false),
//
//                        createStructField("qualCount", createArrayType(DoubleType), false),
//                        createStructField("qualSum", createArrayType(DoubleType), false),
//                        createStructField("qualSumSq", createArrayType(DoubleType), false),
//                createStructField("numSamples", IntegerType, false),
//                createStructField("tiTvRatio", FloatType, false),
//                createStructField("meanQuality", FloatType, false),
//                createStructField("stdDevQuality", FloatType, false),

                createStructField(VARIANT_TYPE_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
                createStructField(VARIANT_BIOTYPE_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
                createStructField(CONSEQUENCE_TYPE_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
                createStructField(CHROMOSOME_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
                createStructField(INDEL_SIZE_COUNTS_COLNAME, createArrayType(createMapType(IntegerType, IntegerType, false)), false),
                createStructField(GENOTYPE_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),

                createStructField(LOF_GENE_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
                createStructField(TRAIT_COUNTS_COLNAME, createArrayType(createMapType(StringType, IntegerType, false)), false),
        });


        public static void initialize(MutableAggregationBuffer buffer, List<String> samples) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(SAMPLES_COLNAME), asScalaBuffer(samples).toSeq());

            List<Integer> numVariants = new LinkedList<>();
            List<Integer> numPass = new LinkedList<>();
            List<Integer> transitions = new LinkedList<>();
            List<Integer> transversions = new LinkedList<>();
            List<Map<String, Integer>> variantTypeCounts = new ArrayList<>();
            List<Map<String, Integer>> variantBiotypeCounts = new ArrayList<>();
            List<Map<String, Integer>> consequenceTypeCounts = new ArrayList<>();
            List<Map<String, Integer>> byChromosomeCounts = new ArrayList<>();
            List<Map<Integer, Integer>> byIndelSizeCounts = new ArrayList<>();
            List<Map<String, Integer>> genotypeCounts = new ArrayList<>();
            List<Map<String, Integer>> lofGeneCounts = new ArrayList<>();
            List<Map<String, Integer>> traitCounts = new ArrayList<>();

            for (int i = 0; i < samples.size(); i++) {
                numVariants.add(0);
                numPass.add(0);
                transitions.add(0);
                transversions.add(0);
                variantTypeCounts.add(new scala.collection.mutable.HashMap<>());
                variantBiotypeCounts.add(new scala.collection.mutable.HashMap<>());
                consequenceTypeCounts.add(new scala.collection.mutable.HashMap<>());
                byChromosomeCounts.add(new scala.collection.mutable.HashMap<>());
                byIndelSizeCounts.add(new scala.collection.mutable.HashMap<>());
                genotypeCounts.add(new scala.collection.mutable.HashMap<>());
                lofGeneCounts.add(new scala.collection.mutable.HashMap<>());
                traitCounts.add(new scala.collection.mutable.HashMap<>());
            }

            setNumVariants(buffer, asScalaBuffer(numVariants).toSeq());
            setNumPass(buffer, asScalaBuffer(numPass).toSeq());
            setTransitions(buffer, asScalaBuffer(transitions).toSeq());
            setTransversions(buffer, asScalaBuffer(transversions).toSeq());
//            setQualCount(buffer, 0);
//            setQualSum(buffer, 0);
//            setQualSumSq(buffer, 0);
//
            setVariantTypeCounts(buffer, asScalaBuffer(variantTypeCounts).toSeq());
            setVariantBiotypeCounts(buffer, asScalaBuffer(variantBiotypeCounts).toSeq());
            setConsequenceTypeCounts(buffer, asScalaBuffer(consequenceTypeCounts).toSeq());
            setByChromosomeCounts(buffer, asScalaBuffer(byChromosomeCounts).toSeq());
            setByIndelSizeCounts(buffer, asScalaBuffer(byIndelSizeCounts).toSeq());
            setGenotypeCounts(buffer, asScalaBuffer(genotypeCounts).toSeq());
            setLofGeneCounts(buffer, asScalaBuffer(lofGeneCounts).toSeq());
            setTraitCounts(buffer, asScalaBuffer(traitCounts).toSeq());
        }

        public static void merge(MutableAggregationBuffer buffer, Row other, int size) {
            List<Integer> numVariants = new LinkedList<>();
            List<Integer> numPass = new LinkedList<>();
            List<Integer> transitions = new LinkedList<>();
            List<Integer> transversions = new LinkedList<>();
            List<Map<String, Integer>> variantTypeCounts = new ArrayList<>();
            List<Map<String, Integer>> variantBiotypeCounts = new ArrayList<>();
            List<Map<String, Integer>> consequenceTypeCounts = new ArrayList<>();
            List<Map<String, Integer>> byChromosomeCounts = new ArrayList<>();
            List<Map<Integer, Integer>> byIndelSizeCounts = new ArrayList<>();
            List<Map<String, Integer>> genotypeCounts = new ArrayList<>();
            List<Map<String, Integer>> lofGeneCounts = new ArrayList<>();
            List<Map<String, Integer>> traitCounts = new ArrayList<>();

            for (int i = 0; i < size; i++) {
                numVariants.add(getNumVariants(buffer).apply(i) + getNumVariants(other).apply(i));
                numPass.add(getNumPass(buffer).apply(i) + getNumPass(other).apply(i));
                transitions.add(getTransitions(buffer).apply(i) + getTransitions(other).apply(i));
                transversions.add(getTransversions(buffer).apply(i) + getTransversions(other).apply(i));
                variantTypeCounts.add(getVariantTypeCounts(buffer).apply(i).$plus$plus(getVariantTypeCounts(other).apply(i)));
                variantBiotypeCounts.add(getVariantBiotypeCounts(buffer).apply(i).$plus$plus(getVariantBiotypeCounts(other).apply(i)));
                consequenceTypeCounts.add(getConsequenceTypeCounts(buffer).apply(i).$plus$plus(getConsequenceTypeCounts(other).apply(i)));
                byChromosomeCounts.add(getByChromosomeCounts(buffer).apply(i).$plus$plus(getByChromosomeCounts(other).apply(i)));
                byIndelSizeCounts.add(getByIndelSizeCounts(buffer).apply(i).$plus$plus(getByIndelSizeCounts(other).apply(i)));
                genotypeCounts.add(getGenotypeCounts(buffer).apply(i).$plus$plus(getGenotypeCounts(other).apply(i)));
                lofGeneCounts.add(getLofGeneCounts(buffer).apply(i).$plus$plus(getLofGeneCounts(other).apply(i)));
                traitCounts.add(getTraitCounts(buffer).apply(i).$plus$plus(getTraitCounts(other).apply(i)));
            }

            setNumVariants(buffer, asScalaBuffer(numVariants).toSeq());
            setNumPass(buffer, asScalaBuffer(numPass).toSeq());
            setTransitions(buffer, asScalaBuffer(transitions).toSeq());
            setTransversions(buffer, asScalaBuffer(transversions).toSeq());
            setVariantTypeCounts(buffer, asScalaBuffer(variantTypeCounts).toSeq());
            setVariantBiotypeCounts(buffer, asScalaBuffer(variantBiotypeCounts).toSeq());
            setConsequenceTypeCounts(buffer, asScalaBuffer(consequenceTypeCounts).toSeq());
            setByChromosomeCounts(buffer, asScalaBuffer(byChromosomeCounts).toSeq());
            setByIndelSizeCounts(buffer, asScalaBuffer(byIndelSizeCounts).toSeq());
            setGenotypeCounts(buffer, asScalaBuffer(genotypeCounts).toSeq());
            setLofGeneCounts(buffer, asScalaBuffer(lofGeneCounts).toSeq());
            setTraitCounts(buffer, asScalaBuffer(traitCounts).toSeq());



//            addQualCount(buffer, getQualCount(other));
//            addQualSum(buffer, getQualSum(other));
//            addQualSumSq(buffer, getQualSumSq(other));
        }

        public static Seq<String> getSamples(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(SAMPLES_COLNAME));
        }

        public static Seq<Integer> getNumVariants(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(NUM_VARIANTS_COLNAME));
        }

        public static void setNumVariants(MutableAggregationBuffer buffer, Seq<Integer> values) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(NUM_VARIANTS_COLNAME), values);
        }

        public static Seq<Integer> getNumPass(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(NUM_PASS_COLNAME));
        }

        public static void setNumPass(MutableAggregationBuffer buffer, Seq<Integer> values) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(NUM_PASS_COLNAME), values);
        }

        public static Seq<Integer> getTransitions(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRANSITIONS_COLNAME));
        }

        public static void setTransitions(MutableAggregationBuffer buffer, Seq<Integer> values) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRANSITIONS_COLNAME), values);
        }

        public static Seq<Integer> getTransversions(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRANSVERSIONS_COLNAME));
        }

        public static void setTransversions(MutableAggregationBuffer buffer, Seq<Integer> values) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRANSVERSIONS_COLNAME), values);
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

        public static Seq<Map<String, Integer>> getVariantTypeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(VARIANT_TYPE_COUNTS_COLNAME));
        }

        public static void setVariantTypeCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(VARIANT_TYPE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getVariantBiotypeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(VARIANT_BIOTYPE_COUNTS_COLNAME));
        }

        public static void setVariantBiotypeCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(VARIANT_BIOTYPE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getConsequenceTypeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(CONSEQUENCE_TYPE_COUNTS_COLNAME));
        }

        public static void setConsequenceTypeCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(CONSEQUENCE_TYPE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getByChromosomeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(CHROMOSOME_COUNTS_COLNAME));
        }

        public static void setByChromosomeCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(CHROMOSOME_COUNTS_COLNAME), value);
        }

        public static Seq<Map<Integer, Integer>> getByIndelSizeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(INDEL_SIZE_COUNTS_COLNAME));
        }

        public static void setByIndelSizeCounts(MutableAggregationBuffer buffer, Seq<Map<Integer, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(INDEL_SIZE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getGenotypeCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(GENOTYPE_COUNTS_COLNAME));
        }

        public static void setGenotypeCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(GENOTYPE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getLofGeneCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(LOF_GENE_COUNTS_COLNAME));
        }

        public static void setLofGeneCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(LOF_GENE_COUNTS_COLNAME), value);
        }

        public static Seq<Map<String, Integer>> getTraitCounts(Row row) {
            return row.getSeq(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRAIT_COUNTS_COLNAME));
        }

        public static void setTraitCounts(MutableAggregationBuffer buffer, Seq<Map<String, Integer>> value) {
            buffer.update(VARIANT_SAMPLE_STATS_BUFFER_SCHEMA.fieldIndex(TRAIT_COUNTS_COLNAME), value);
        }
    }
}
