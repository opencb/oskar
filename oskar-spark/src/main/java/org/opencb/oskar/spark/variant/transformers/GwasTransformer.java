package org.opencb.oskar.spark.variant.transformers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.analysis.stats.ChiSquareTest;
import org.opencb.oskar.analysis.stats.ChiSquareTestResult;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.stats.FisherTestResult;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.transformers.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

public class GwasTransformer extends AbstractTransformer implements HasStudyId {

    public static final String GWAS_COL_NAME = "gwas_stats";

    private final Param<List<String>> sampleList1Param;
    private final Param<List<String>> sampleList2Param;
    private final Param<String> phenotype1Param;
    private final Param<String> phenotype2Param;
    private final Param<String> cohort1Param;
    private final Param<String> cohort2Param;
    private final Param<String> methodParam;
    private final Param<String> fisherModeParam;

    public GwasTransformer() {
        this(null);
    }

    public GwasTransformer(String uid) {
        super(uid);
        sampleList1Param = new Param<>(this, "sampleList1", "Sample list 1");
        sampleList2Param = new Param<>(this, "sampleList2", "Sample list 2");
        phenotype1Param = new Param<>(this, "phenotype1", "Phenotype 1");
        phenotype2Param = new Param<>(this, "phenotype2", "Phenotype 2");
        cohort1Param = new Param<>(this, "cohort1", "Cohort 1");
        cohort2Param = new Param<>(this, "cohort2", "Cohort 2");
        methodParam = new Param<>(this, "method", "Method: fisher or chi-square");
        fisherModeParam = new Param<>(this, "fisherMode", "Fisher exact test mode");

        setDefault(sampleList1Param(), Collections.emptyList());
        setDefault(sampleList2Param(), Collections.emptyList());
        setDefault(phenotype1Param(), "");
        setDefault(phenotype2Param(), "");
        setDefault(cohort1Param(), "");
        setDefault(cohort2Param(), "");
        setDefault(methodParam(), "fisher");
        setDefault(fisherModeParam(), "two-side");
    }

    // Study ID

    @Override
    public GwasTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    // Sample lists: 1, 2

    public Param<List<String>> sampleList1Param() {
        return sampleList1Param;
    }
    public GwasTransformer setSampleList1(List<String> sampleList) {
        set(sampleList1Param, sampleList);
        return this;
    }
    public List<String> getSampleList1() {
        return getOrDefault(sampleList1Param);
    }

    public Param<List<String>> sampleList2Param() {
        return sampleList2Param;
    }
    public GwasTransformer setSampleList2(List<String> sampleList) {
        set(sampleList2Param, sampleList);
        return this;
    }
    public List<String> getSampleList2() {
        return getOrDefault(sampleList2Param);
    }

    // Phenotypes: 1, 2

    public Param<String> phenotype1Param() {
        return phenotype1Param;
    }
    public GwasTransformer setPhenotype1(String phenotype1) {
        set(phenotype1Param, phenotype1);
        return this;
    }
    public String getPhenotype1() {
        return getOrDefault(phenotype1Param);
    }

    public Param<String> phenotype2Param() {
        return phenotype2Param;
    }
    public GwasTransformer setPhenotype2(String phenotype2) {
        set(phenotype2Param, phenotype2);
        return this;
    }
    public String getPhenotype2() {
        return getOrDefault(phenotype2Param);
    }

    // Cohorts: 1, 2

    public Param<String> cohort1Param() {
        return cohort1Param;
    }
    public GwasTransformer setCohort1(String cohort1) {
        set(cohort1Param, cohort1);
        return this;
    }
    public String getCohort1() {
        return getOrDefault(cohort1Param);
    }

    public Param<String> cohort2Param() {
        return cohort2Param;
    }
    public GwasTransformer setCohort2(String cohort2) {
        set(cohort2Param, cohort2);
        return this;
    }
    public String getCohort2() {
        return getOrDefault(cohort2Param);
    }

    // GWAS method: fisher, chi-square

    public Param<String> methodParam() {
        return methodParam;
    }
    public GwasTransformer setMethod(String method) {
        set(methodParam, method);
        return this;
    }
    public String getMethod() {
        return getOrDefault(methodParam);
    }

    // Fisher mode

    public Param<String> fisherModeParam() {
        return fisherModeParam;
    }
    public GwasTransformer setFisherMode(String fisherMode) {
        set(fisherModeParam, fisherMode);
        return this;
    }
    public String getFisherMode() {
        return getOrDefault(fisherModeParam);
    }

    // Main function

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        // Search affected samples (and take the index)
        Set<Integer> affectedIndexSet = new HashSet<>();
        Set<Integer> unaffectedIndexSet = new HashSet<>();
        List<String> samples = new Oskar().metadata().samples(df, getStudyId());

        if (StringUtils.isNotEmpty(getPhenotype1()) || StringUtils.isNotEmpty(getPhenotype2())) {
            // Get affected/unaffected samples from phenotypes
            List<Pedigree> pedigrees = new Oskar().metadata().pedigrees(df, getStudyId());
            for (Pedigree pedigree: pedigrees) {
                for (Member member: pedigree.getMembers()) {
                    if (ListUtils.isNotEmpty(member.getPhenotypes())) {
                        for (Phenotype phenotype: member.getPhenotypes()) {
                            if (StringUtils.isNotEmpty(getPhenotype1()) && getPhenotype1().equals(phenotype.getId())) {
                                affectedIndexSet.add(samples.indexOf(member.getId()));
                                break;
                            } else if (StringUtils.isNotEmpty(getPhenotype2()) && getPhenotype2().equals(phenotype.getId())) {
                                unaffectedIndexSet.add(samples.indexOf(member.getId()));
                                break;
                            }
                        }
                    }
                }
            }
            // Check if only one phenotype is present
            if (StringUtils.isEmpty(getPhenotype1()) && CollectionUtils.isNotEmpty(unaffectedIndexSet)) {
                populateTargetIndexSet(samples, unaffectedIndexSet, affectedIndexSet);
            } else if (StringUtils.isEmpty(getPhenotype2()) && CollectionUtils.isNotEmpty(affectedIndexSet)) {
                populateTargetIndexSet(samples, affectedIndexSet, unaffectedIndexSet);
            }
        } else if (CollectionUtils.isNotEmpty(getSampleList1()) || CollectionUtils.isNotEmpty(getSampleList2())) {
            // Get affected/unaffected samples from lists
            getSampleList1().forEach(s -> affectedIndexSet.add(samples.indexOf(s)));
            getSampleList2().forEach(s -> unaffectedIndexSet.add(samples.indexOf(s)));

            // Check if only one sample list is present
            if (CollectionUtils.isEmpty(getSampleList1()) && CollectionUtils.isNotEmpty(unaffectedIndexSet)) {
                populateTargetIndexSet(samples, unaffectedIndexSet, affectedIndexSet);
            } else if (CollectionUtils.isEmpty(getSampleList2()) && CollectionUtils.isNotEmpty(affectedIndexSet)) {
                populateTargetIndexSet(samples, affectedIndexSet, unaffectedIndexSet);
            }
        } else if (StringUtils.isNotEmpty(getCohort1()) || StringUtils.isNotEmpty(getCohort2())) {
            // Get affected/unaffected samples from cohorts
            List<Cohort> cohorts = null;
            List<VariantStudyMetadata> studies = new Oskar().metadata().variantMetadata(df).getStudies();
            for (VariantStudyMetadata variantStudyMetadata : studies) {
                if (variantStudyMetadata.getId().equals(getStudyId())) {
                    cohorts = variantStudyMetadata.getCohorts();
                    break;
                }
            }

            if (CollectionUtils.isNotEmpty(cohorts)) {
                for (Cohort cohort : cohorts) {
                    if (cohort.getId().equals(getCohort1())) {
                        cohort.getSampleIds().forEach(s -> affectedIndexSet.add(samples.indexOf(s)));
                    } else if (cohort.getId().equals(getCohort2())) {
                        cohort.getSampleIds().forEach(s -> unaffectedIndexSet.add(samples.indexOf(s)));
                    }
                }

                // Check if only one cohort is present
                if (StringUtils.isEmpty(getCohort1()) && CollectionUtils.isNotEmpty(unaffectedIndexSet)) {
                    populateTargetIndexSet(samples, unaffectedIndexSet, affectedIndexSet);
                } else if (StringUtils.isEmpty(getCohort2()) && CollectionUtils.isNotEmpty(affectedIndexSet)) {
                    populateTargetIndexSet(samples, affectedIndexSet, unaffectedIndexSet);
                }
            }
        }

        // Select UDF according to the GWAS method (fisher or chi-square)
        UserDefinedFunction udf = null;
        if ("fisher-test".equals(getMethod())) {
            udf = udf(new GwasTransformer.FisherFunction(getStudyId(), FisherExactTest.TWO_SIDED, affectedIndexSet,
                    unaffectedIndexSet), fisherSchema());

        } else if ("chi-square-test".equals(getMethod())) {
            udf = udf(new GwasTransformer.ChiSquareFunction(getStudyId(), affectedIndexSet, unaffectedIndexSet),
                    chiSquareSchema());
        }

        return dataset.withColumn(GWAS_COL_NAME, udf.apply(new ListBuffer<Column>().$plus$eq(col("studies"))))
                .selectExpr("*", GWAS_COL_NAME + ".*").drop(GWAS_COL_NAME);
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        if ("fisher-test".equals(getMethod())) {
            fields.add(createStructField(GWAS_COL_NAME, fisherSchema(), false));
        } else if ("chi-square-test".equals(getMethod())) {
            fields.add(createStructField(GWAS_COL_NAME, chiSquareSchema(), false));
        }
        return createStructType(fields);
    }

    private StructType fisherSchema() {
        return createStructType(new StructField[]{
                createStructField("pValue", DoubleType, false),
                createStructField("oddRatio", DoubleType, false),
        });
    }

    private StructType chiSquareSchema() {
        return createStructType(new StructField[]{
                createStructField("chiSquare", DoubleType, false),
                createStructField("pValue", DoubleType, false),
                createStructField("oddRatio", DoubleType, false),
        });
    }

    private void populateTargetIndexSet(List<String> samples, Set<Integer> source, Set<Integer> target) {
        for (int i = 0; i < samples.size(); i++) {
            if (!source.contains(i)) {
                target.add(i);
            }
        }
    }

    public static class FisherFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>, Row>
            implements Serializable {
        private final String studyId;
        private final int mode;
        private final Set<Integer> affectedIndexSet;
        private final Set<Integer> unaffectedIndexSet;

        public FisherFunction(String studyId, int mode, Set<Integer> affectedIndexSet, Set<Integer> unaffectedIndexSet) {
            this.studyId = studyId;
            this.mode = mode;
            this.affectedIndexSet = affectedIndexSet;
            this.unaffectedIndexSet = unaffectedIndexSet;
        }

        @Override
        public Row apply(WrappedArray<GenericRowWithSchema> studies) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

            int[] counts = computeCounts(study, affectedIndexSet, unaffectedIndexSet);
            FisherTestResult result = new FisherExactTest().fisherTest(counts[0], counts[1], counts[2], counts[3], mode);

            return RowFactory.create(result.getpValue(), result.getOddRatio());
        }
    }

    public static class ChiSquareFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>, Row>
            implements Serializable {
        private final String studyId;
        private final Set<Integer> affectedIndexSet;
        private final Set<Integer> unaffectedIndexSet;

        public ChiSquareFunction(String studyId, Set<Integer> affectedIndexSet, Set<Integer> unaffectedIndexSet) {
            this.studyId = studyId;
            this.affectedIndexSet = affectedIndexSet;
            this.unaffectedIndexSet = unaffectedIndexSet;
        }

        @Override
        public Row apply(WrappedArray<GenericRowWithSchema> studies) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

            int[] counts = computeCounts(study, affectedIndexSet, unaffectedIndexSet);
            ChiSquareTestResult result = ChiSquareTest.chiSquareTest(counts[0], counts[1], counts[2], counts[3]);

            return RowFactory.create(result.getChiSquare(), result.getpValue(), result.getOddRatio());
        }
    }

    private static int[] computeCounts(GenericRowWithSchema study, Set<Integer> affectedIndexSet, Set<Integer> unaffectedIndexSet) {
        int a = 0; // case #REF
        int b = 0; // control #REF
        int c = 0; // case #ALT
        int d = 0; // control #ALT

        List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
        for (int i = 0; i < samplesData.size(); i++) {
            WrappedArray<String> sampleData = samplesData.get(i);
            MendelianError.GenotypeCode gtCode = MendelianError.getAlternateAlleleCount(new Genotype(sampleData.apply(0)));
            switch (gtCode) {
                case HOM_REF: {
                    if (affectedIndexSet.contains(i)) {
                        a += 2;
                    } else if (unaffectedIndexSet.contains(i)) {
                        b += 2;
                    }
                    break;
                }
                case HET: {
                    if (affectedIndexSet.contains(i)) {
                        a++;
                        c++;
                    } else if (unaffectedIndexSet.contains(i)) {
                        b++;
                        d++;
                    }
                    break;
                }
                case HOM_VAR:
                    if (affectedIndexSet.contains(i)) {
                        c += 2;
                    } else if (unaffectedIndexSet.contains(i)) {
                        d += 2;
                    }
                    break;
                default:
                    break;
            }
        }
        return new int[]{a, b, c, d};
    }
}
