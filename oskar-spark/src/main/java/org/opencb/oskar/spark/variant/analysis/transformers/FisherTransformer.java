package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.stats.FisherTestResult;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.params.HasPhenotype;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
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

public class FisherTransformer extends AbstractTransformer implements HasStudyId, HasPhenotype {

    public static final String FISHER_COL_NAME = "p_value__odd_ratio";

    private final Param<List<String>> sampleList1Param;
    private final Param<List<String>> sampleList2Param;
    private final Param<Integer> modeParam;

    public FisherTransformer() {
        this(null);
    }

    public FisherTransformer(String uid) {
        super(uid);
        modeParam = new Param<>(this, "mode", "Fisher exact test mode");
        sampleList1Param = new Param<>(this, "sampleList1", "Sample list 1");
        sampleList2Param = new Param<>(this, "sampleList2", "Sample list 2");

        setDefault(phenotypeParam(), "");
        setDefault(modeParam, FisherExactTest.TWO_SIDED);
        setDefault(sampleList1Param(), Collections.emptyList());
        setDefault(sampleList2Param(), Collections.emptyList());
    }

    // Study ID

    @Override
    public FisherTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    // Phenotype

    @Override
    public FisherTransformer setPhenotype(String phenotype) {
        set(phenotypeParam(), phenotype);
        return this;
    }

    // Mode

    public Param<Integer> modeParam() {
        return modeParam;
    }

    public FisherTransformer setMode(int mode) {
        set(modeParam, mode);
        return this;
    }

    public int getMode() {
        return getOrDefault(modeParam);
    }

    // Sample list 1

    public Param<List<String>> sampleList1Param() {
        return sampleList1Param;
    }

    public FisherTransformer setSampleList1(List<String> sampleList) {
        set(sampleList1Param, sampleList);
        return this;
    }

    public List<String> getSampleList1() {
        return getOrDefault(sampleList1Param);
    }

    // Sample list 2

    public Param<List<String>> sampleList2Param() {
        return sampleList2Param;
    }

    public FisherTransformer setSampleList2(List<String> sampleList) {
        set(sampleList2Param, sampleList);
        return this;
    }

    public List<String> getSampleList2() {
        return getOrDefault(sampleList2Param);
    }

    // Main function

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        // Search affected samples (and take the index)
        Set<Integer> affectedIndexSet = new HashSet<>();
        List<String> samples = new Oskar().metadata().samples(df, getStudyId());
        Set<Integer> unaffectedIndexSet = new HashSet<>();

        if (StringUtils.isNotEmpty(getPhenotype())) {
            // Processing from phenotype
            List<Pedigree> pedigrees = new Oskar().metadata().pedigrees(df, getStudyId());
            for (Pedigree pedigree: pedigrees) {
                for (Member member: pedigree.getMembers()) {
                    if (ListUtils.isNotEmpty(member.getPhenotypes())) {
                        for (Phenotype phenotype: member.getPhenotypes()) {
                            if (getPhenotype().equals(phenotype.getId())) {
                                affectedIndexSet.add(samples.indexOf(member.getId()));
                                break;
                            }
                        }
                    }
                }
            }
        } else if (CollectionUtils.isNotEmpty(getSampleList1()) && CollectionUtils.isNotEmpty(getSampleList2())) {
            // Processing from two lists
            for (String sampleId : getSampleList1()) {
                affectedIndexSet.add(samples.indexOf(sampleId));
            }
            for (String sampleId : getSampleList2()) {
                unaffectedIndexSet.add(samples.indexOf(sampleId));
            }
        }

        UserDefinedFunction fisher = udf(new FisherTransformer.FisherFunction(getStudyId(), getMode(), affectedIndexSet,
                        unaffectedIndexSet), DataTypes.createArrayType(DoubleType));

        return dataset.withColumn(FISHER_COL_NAME, fisher.apply(new ListBuffer<Column>().$plus$eq(col("studies"))));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField(FISHER_COL_NAME, createArrayType(DoubleType, false), false));
        return createStructType(fields);
    }

    public static class FisherFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>, WrappedArray<Double>>
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
        public WrappedArray<Double> apply(WrappedArray<GenericRowWithSchema> studies) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

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

            FisherTestResult fisherTestResult = new FisherExactTest().fisherTest(a, b, c, d, mode);

            double[] res = new double[2];
            res[0] = fisherTestResult.getpValue();
            res[1] = fisherTestResult.getOddRatio();

            return WrappedArray.make(res);
        }
    }
}
