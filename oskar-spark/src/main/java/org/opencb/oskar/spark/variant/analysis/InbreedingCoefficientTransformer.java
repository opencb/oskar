package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.SAMPLES_DATA_IDX;
import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.STATS_IDX;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.study;


/**
 * Created on 27/09/18.
 *
 * Count observed and expected autosomal homozygous genotype for each sample, and report method-of-moments F coefficient
 * estimates. (Ritland, Kermit. 1996)
 *
 * Values:
 *  - Total genotypes Count : Total count of genotypes for sample
 *  - Observed homozygotes  : Count of observed homozygote genotypes for each sample, in each variant
 *  - Expected homozygotes  : Count of expected homozygote genotypes for each sample, in each variant.
 *          Calculated with the MAF of the cohort ALL. 1.0−(2.0∗maf∗(1.0−maf))
 *  - F                     : Inbreeding coefficient. Calculated as:
 *          ([observed hom. count] - [expected count]) / ([total genotypes count] - [expected count])
 *
 * Unless otherwise specified, the genotype counts will exclude the missing and multi-allelic genotypes.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class InbreedingCoefficientTransformer extends AbstractTransformer implements HasStudyId {

    private static final StructType STRUCT_TYPE = createStructType(new StructField[]{
            createStructField("SampleId", StringType, false),
            createStructField("F", DoubleType, false),
            createStructField("ObservedHom", IntegerType, false),
            createStructField("ExpectedHom", DoubleType, false),
            createStructField("GenotypesCount", IntegerType, false),
    });

    private final Param<Boolean> missingGenotypesAsHomRefParam;
    private final Param<Boolean> includeMultiAllelicGenotypesParam;
    private final Param<Double> mafThresholdParam;

    public InbreedingCoefficientTransformer() {
        this(null);
    }

    public InbreedingCoefficientTransformer(String uid) {
        super(uid);

        missingGenotypesAsHomRefParam = new Param<>(this, "missingGenotypesAsHomRef",
                "Treat missing genotypes as HomRef genotypes");
        includeMultiAllelicGenotypesParam = new Param<>(this, "includeMultiAllelicGenotypes",
                "Include multi-allelic variants in the calculation");
        mafThresholdParam = new Param<>(this, "mafThreshold",
                "Include multi-allelic variants in the calculation");

        setDefault(missingGenotypesAsHomRefParam, false);
        setDefault(includeMultiAllelicGenotypesParam, false);
        setDefault(mafThresholdParam, 0.0);
        setDefault(studyIdParam(), "");
    }

    public Param<Boolean> missingGenotypesAsHomRefParam() {
        return missingGenotypesAsHomRefParam;
    }

    public InbreedingCoefficientTransformer setMissingGenotypesAsHomRef(boolean missingGenotypesAsHomRef) {
        set(missingGenotypesAsHomRefParam, missingGenotypesAsHomRef);
        return this;
    }

    public Param<Boolean> includeMultiAllelicGenotypesParam() {
        return includeMultiAllelicGenotypesParam;
    }

    public InbreedingCoefficientTransformer setIncludeMultiAllelicGenotypes(boolean includeMultiAllelicGenotypes) {
        set(includeMultiAllelicGenotypesParam, includeMultiAllelicGenotypes);
        return this;
    }

    public Param<Double> mafThresholdParam() {
        return mafThresholdParam;
    }

    public InbreedingCoefficientTransformer setMafThreshold(double mafThreshold) {
        set(mafThresholdParam, mafThreshold);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Map<String, List<String>> samplesMap = new VariantMetadataManager().samples((Dataset<Row>) dataset);
        boolean multiStudy = samplesMap.size() != 1;
        List<String> sampleNames;
        String studyId = getStudyId();
        if (StringUtils.isNotEmpty(studyId)) {
            sampleNames = samplesMap.get(studyId);
            if (sampleNames == null) {
                throw OskarException.unknownStudy(studyId, samplesMap.keySet());
            }
        } else {
            if (multiStudy) {
                throw OskarException.missingStudy(samplesMap.keySet());
            } else {
                studyId = samplesMap.keySet().iterator().next();
                sampleNames = samplesMap.values().iterator().next();
            }
        }
        InbreedingCoefficientUserDefinedAggregationFunction udaf = new InbreedingCoefficientUserDefinedAggregationFunction(
                sampleNames.size(),
                getOrDefault(missingGenotypesAsHomRefParam),
                getOrDefault(includeMultiAllelicGenotypesParam),
                getOrDefault(mafThresholdParam), sampleNames
        );

        Column study;
        if (multiStudy) {
            study = study("studies", studyId);
        } else {
            study = col("studies").apply(0);
        }
        return dataset.agg(udaf.apply(study).as("r"))
                .select(explode(col("r").apply("values")).as("value"))
                .selectExpr("value.*");
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return STRUCT_TYPE;
    }

    public static class InbreedingCoefficientUserDefinedAggregationFunction extends UserDefinedAggregateFunction {

        private final int numSamples;
        private final boolean missingGenotypesAsHomRef;
        private final boolean includeMultiAllelicGenotypes;
        private final List<String> sampleNames;
        private final double mafThreshold;

        public InbreedingCoefficientUserDefinedAggregationFunction(
                int numSamples, boolean missingGenotypesAsHomRef, boolean includeMultiAllelicGenotypes, double mafThreshold,
                List<String> sampleNames) {

            this.numSamples = numSamples;
            this.missingGenotypesAsHomRef = missingGenotypesAsHomRef;
            this.includeMultiAllelicGenotypes = includeMultiAllelicGenotypes;
            this.sampleNames = sampleNames;
            this.mafThreshold = mafThreshold;
        }

        @Override
        public StructType inputSchema() {
            return createStructType(new StructField[]{
                    createStructField("study", VariantToRowConverter.STUDY_DATA_TYPE, false),
            });
        }

        @Override
        public StructType bufferSchema() {
            ArrayList<StructField> fields = new ArrayList<>();
            for (int i = 0; i < numSamples; i++) {
                fields.add(createStructField("expectedHomCount_" + i, DoubleType, false));
                fields.add(createStructField("observedHomCount_" + i, IntegerType, false));
                fields.add(createStructField("count_" + i, IntegerType, false));
            }
            return createStructType(fields);
        }

        @Override
        public DataType dataType() {
            return createStructType(
                    Collections.singletonList(
                            createStructField("values",
                                    createArrayType(
                                            STRUCT_TYPE), false)));
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            for (int i = 0; i < numSamples; i++) {
                buffer.update(i * 3, (double) 0);
                buffer.update(i * 3 + 1, 0);
                buffer.update(i * 3 + 2, 0);
            }
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            Row study = input.getStruct(0);

            Row stats = (Row) study.getMap(STATS_IDX).get("ALL").get();
            float maf;
            if (missingGenotypesAsHomRef) {
                // Recalculate stats with missing as homRef
                int missingAlleleCount = stats.getInt(stats.fieldIndex("missingAlleleCount"));
                int alleleCount = stats.getInt(stats.fieldIndex("alleleCount"));
                int refAlleleCount = stats.getInt(stats.fieldIndex("refAlleleCount"));
                int altAlleleCount = stats.getInt(stats.fieldIndex("altAlleleCount"));

                float refAlleleFreq = ((float) (refAlleleCount + missingAlleleCount)) / (alleleCount + missingAlleleCount);
                float altAlleleFreq = ((float) (altAlleleCount)) / (alleleCount + missingAlleleCount);
                maf = Math.min(refAlleleFreq, altAlleleFreq);
            } else {
                maf = Math.min(stats.getFloat(stats.fieldIndex("refAlleleFreq")),
                        stats.getFloat(stats.fieldIndex("altAlleleFreq")));
            }

            if (maf > mafThreshold) {

                List<WrappedArray<String>> samplesData = study.getList(SAMPLES_DATA_IDX);

                double expected = 1 - (2 * maf * (1 - maf));

                int i = -1;
                for (WrappedArray<String> sample : samplesData) {
                    i++;
                    String gt = sample.apply(0);
                    Genotype genotype = new Genotype(gt);
                    if (!includeMultiAllelicGenotypes && genotype.getCode() == AllelesCode.MULTIPLE_ALTERNATES) {
                        continue;
                    }
                    if (!missingGenotypesAsHomRef && genotype.getCode() == AllelesCode.ALLELES_MISSING) {
                        continue;
                    }

                    // expected
                    buffer.update(i * 3, buffer.getDouble(i * 3) + expected);

                    // observed hom
                    if (isHom(genotype)) {
                        buffer.update(i * 3 + 1, buffer.getInt(i * 3 + 1) + 1);
                    }

                    // total count
                    buffer.update(i * 3 + 2, buffer.getInt(i * 3 + 2) + 1);
                }
            }
        }

        private boolean isHom(Genotype genotype) {
            if (genotype.getCode() == AllelesCode.ALLELES_MISSING) {
                return missingGenotypesAsHomRef;
            }
            int allele = genotype.getAllelesIdx()[0];
            for (int allelesIdx : genotype.getAllelesIdx()) {
                if (allelesIdx != allele) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            for (int i = 0; i < numSamples; i++) {
                buffer1.update(i * 3, buffer1.getDouble(i * 3) + buffer2.getDouble(i * 3));
                buffer1.update(i * 3 + 1, buffer1.getInt(i * 3 + 1) + buffer2.getInt(i * 3 + 1));
                buffer1.update(i * 3 + 2, buffer1.getInt(i * 3 + 2) + buffer2.getInt(i * 3 + 2));
            }
        }

        @Override
        public Object evaluate(Row buffer) {
            List<Row> values = new ArrayList<>(numSamples);
            for (int i = 0; i < numSamples; i++) {
                double expectedHomCount = buffer.getDouble(i * 3);
                int observedHomCount = buffer.getInt(i * 3 + 1);
                int totalCount = buffer.getInt(i * 3 + 2);

                double f = ((float) (observedHomCount - expectedHomCount)) / (totalCount - expectedHomCount);
                values.add(new GenericRow(new Object[]{
                        sampleNames.get(i),
                        f,
                        observedHomCount,
                        expectedHomCount,
                        totalCount,
                }));
            }


            return new GenericRow(new Object[]{values});
        }
    }


}
