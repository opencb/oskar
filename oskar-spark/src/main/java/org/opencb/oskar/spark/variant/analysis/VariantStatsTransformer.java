package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.opencb.oskar.spark.variant.converters.VariantToRowConverter.SAMPLES_DATA_IDX;

/**
 * Created on 08/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsTransformer extends AbstractTransformer {

    private final Param<String> cohortParam;
    private final Param<String> studyIdParam;
    private final Param<List<String>> samplesParam;
    private final Param<Boolean> missingAsReferenceParam;

    public VariantStatsTransformer(String studyId, String cohort, List<String> samples) {
        this(null);
        if (cohort != null) {
            setCohort(cohort);
        }
        if (studyId != null) {
            setStudyId(studyId);
        }
        if (samples != null) {
            setSamples(samples);
        }
    }

    public VariantStatsTransformer() {
        this(null);
    }

    public VariantStatsTransformer(String uid) {
        super(uid);
        studyIdParam = new Param<>(this, "studyId",
                "Id of the study to calculate the stats from.");
        cohortParam = new Param<>(this, "cohort",
                "Name of the cohort to calculate stats from. By default, " + StudyEntry.DEFAULT_COHORT);
        samplesParam = new Param<>(this, "samples",
                "Samples belonging to the cohort. If empty, will try to read from metadata. "
                        + "If missing, will use all samples from the dataset.");
        missingAsReferenceParam = new Param<>(this, "missingAsReference",
                "Count missing alleles as reference alleles.");

        setDefault(cohortParam, StudyEntry.DEFAULT_COHORT);
        setDefault(studyIdParam, "");
        setDefault(samplesParam, Collections.emptyList());
        setDefault(missingAsReferenceParam, false);

    }

    public Param<String> cohortParam() {
        return cohortParam;
    }

    public VariantStatsTransformer setCohort(String cohort) {
        set(cohortParam(), cohort);
        return this;
    }

    public String getCohort() {
        return getOrDefault(cohortParam());
    }

    public Param<String> studyIdParam() {
        return studyIdParam;
    }

    public VariantStatsTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    public Param<List<String>> samplesParam() {
        return samplesParam;
    }

    public VariantStatsTransformer setSamples(List<String> samples) {
        set(samplesParam(), samples);
        return this;
    }

    public List<String> getSamples() {
        return getOrDefault(samplesParam());
    }

    public Param<Boolean> missingAsReferenceParam() {
        return missingAsReferenceParam;
    }

    public VariantStatsTransformer setMissingAsReference(Boolean missingAsReference) {
        set(missingAsReferenceParam(), missingAsReference);
        return this;
    }

    public Boolean getMissingAsReference() {
        return getOrDefault(missingAsReferenceParam());
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;
        VariantMetadataManager vmm = new VariantMetadataManager();


        String studyId = getStudyId();
        Map<String, List<String>> samplesMap = vmm.samples(df);
        if (StringUtils.isEmpty(studyId)) {
            studyId = samplesMap.keySet().iterator().next();
        }

        List<String> samples = getSamples();
        Set<Integer> sampleIdx;
        if (!CollectionUtils.isNotEmpty(samples)) {
            sampleIdx = Collections.emptySet();
        } else {
            sampleIdx = new HashSet<>(samples.size());
            List<String> sampleNames = samplesMap.get(studyId);
            for (String sample : samples) {
                int idx = sampleNames.indexOf(sample);
                if (idx < 0) {
                    throw new IllegalArgumentException("Sample \"" + sample + "\" not found in study \"" + studyId + "\".");
                }
                sampleIdx.add(idx);
            }
        }

        // We want to preserve the metadata, and add the new Cohort to the VariantMetadata.
        // Use the new dataType with the modified metadata as "returnDataType" of the UDF.
        DataType dataTypeWithNewMetadata;
        try {
            VariantMetadata variantMetadata = vmm.variantMetadata(df);
            for (VariantStudyMetadata study : variantMetadata.getStudies()) {
                if (study.getId().equals(studyId)) {
                    study.getCohorts().add(new Cohort(getCohort(), getSamples(), SampleSetType.UNKNOWN));
                }
            }
            // Only get resulting data type. Avoid unnecessary casts
            dataTypeWithNewMetadata = vmm.setVariantMetadata(df, variantMetadata).schema().apply("studies").dataType();
        } catch (OskarException e) {
            throw new IllegalStateException(e);
        }

        UserDefinedFunction statsFromStudy = udf(
                new VariantStatsFromStudiesFunction(studyId, getCohort(), sampleIdx, getMissingAsReference()), dataTypeWithNewMetadata);

        return df.withColumn("studies", statsFromStudy.apply(new ListBuffer<Column>()
                .$plus$eq(col("reference"))
                .$plus$eq(col("alternate"))
                .$plus$eq(col("studies"))
        ));
    }

    public static class VariantStatsFromStudiesFunction
            extends AbstractFunction3<String, String, WrappedArray<Row>, WrappedArray<Row>> implements Serializable {

        private final String cohortName;
        private final String studyId;
        private final Set<Integer> samplesIdx;
        private final Boolean missingAsReference;
        //        List<Integer> samplePositions;
        private final VariantToRowConverter converter = new VariantToRowConverter();

        public VariantStatsFromStudiesFunction(String studyId, String cohortName, Set<Integer> samplesIdx, Boolean missingAsReference) {
            this.cohortName = cohortName;
            this.studyId = studyId;
            this.samplesIdx = samplesIdx;
            this.missingAsReference = missingAsReference;
        }

        @Override
        public WrappedArray<Row> apply(String reference, String alternate, WrappedArray<Row> studies) {

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
                    return studies;
                }
            }
            Map<String, Row> stats = calculateStats(cohortName, reference, alternate, study);

            int statsIdx = study.fieldIndex("stats");

            Object[] values = new Object[study.length()];
            for (int i = 0; i < study.length(); i++) {
                if (i == statsIdx) {
                    values[i] = stats;
                } else {
                    values[i] = study.get(i);
                }
            }
            Row newStudy = new GenericRowWithSchema(values, study.schema());

            return WrappedArray.<Row>make(new Row[]{newStudy});
        }

        private Map<String, Row> calculateStats(String cohortName, String reference, String alternate, Row study) {

            List<WrappedArray<String>> samplesData = study.getList(SAMPLES_DATA_IDX);

            Map<String, Integer> map = new HashMap<>();
            if (samplesIdx != null && !samplesIdx.isEmpty()) {
                for (Integer sampleIdx : samplesIdx) {
                    map.compute(samplesData.get(sampleIdx).apply(0), (k, i) -> i == null ? 1 : i + 1);
                }
            } else {
                for (WrappedArray<String> sampleData : samplesData) {
                    map.compute(sampleData.apply(0), (k, i) -> i == null ? 1 : i + 1);
                }
            }
            Map<Genotype, Integer> gtMap = new HashMap<>();
            map.forEach((gtStr, count) -> {
                Genotype gt;
                if (gtStr.equals("?/?")) {
                    gt = new Genotype(missingAsReference ? "0/0" : "./.");
                } else if (missingAsReference && gtStr.equals("./.")) {
                    gt = new Genotype("0/0");
                } else {
                    gt = new Genotype(gtStr);
                    if (missingAsReference && gt.getCode().equals(AllelesCode.ALLELES_MISSING)) {
                        gt = new Genotype(gtStr.replace('.', '0'));
                    }
                }
                gtMap.compute(gt, (k, i) -> i == null ? count : i + count);
            });

            VariantStats variantStats = new VariantStats();
            VariantStatsCalculator.calculate(gtMap, variantStats, reference, alternate);
            Map<String, Row> stats = new HashMap<>(study.getJavaMap(study.fieldIndex("stats")));

            Row row = converter.convert(variantStats.getImpl());

            stats.put(cohortName, row);
            return stats;
        }


    }
}
