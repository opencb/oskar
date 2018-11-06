package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.avro.StudyEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

/**
 * Created on 08/06/18.
 *
 * TODO: Preserve metadata from studies!
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStatsTransformer extends AbstractTransformer {

    private Param<String> cohortParam;
    private Param<String> studyIdParam;
    private Param<List<String>> samplesParam;

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
        setDefault(cohortParam(), "ALL");
        setDefault(studyIdParam(), "");
        setDefault(samplesParam(), Collections.emptyList());
    }

    public Param<String> cohortParam() {
        return cohortParam = cohortParam == null ? new Param<>(this, "cohort", "") : cohortParam;
    }

    public VariantStatsTransformer setCohort(String cohort) {
        set(cohortParam(), cohort);
        return this;
    }

    public String getCohort() {
        return getOrDefault(cohortParam());
    }

    public Param<String> studyIdParam() {
        return studyIdParam = studyIdParam == null ? new Param<>(this, "studyId", "") : studyIdParam;
    }

    public VariantStatsTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    public Param<List<String>> samplesParam() {
        return samplesParam = samplesParam == null ? new Param<>(this, "samples", "") : samplesParam;
    }

    public VariantStatsTransformer setSamples(List<String> samples) {
        set(samplesParam(), samples);
        return this;
    }

    public List<String> getSamples() {
        return getOrDefault(samplesParam());
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        List<String> samples = getSamples();
        Set<Integer> sampleIdx;
        String studyId = getStudyId();
        if (samples != null && !samples.isEmpty()) {
            sampleIdx = new HashSet<>(samples.size());
            Metadata metadata = dataset.schema().apply("studies").metadata();
            Metadata samplesMetadata = metadata.getMetadata("samples");
            if (StringUtils.isEmpty(studyId)) {
                studyId = samplesMetadata.map().keySet().iterator().next();
            }
            String[] sampleNames = samplesMetadata.getStringArray(studyId);

            for (String sample : samples) {
                int idx = Arrays.binarySearch(sampleNames, sample);
                if (idx < 0) {
                    throw new IllegalArgumentException("Sample \"" + sample + "\" not found in study \"" + studyId + "\".");
                }
                sampleIdx.add(idx);
            }
        } else {
            sampleIdx = Collections.emptySet();
        }

//
//        datasetRow.withColumn("studies[0].stats", new Column("studies[0].stats"));
//
//        UserDefinedFunction mode = udf((Function1<Row, String>) (Row ss) -> "", DataTypes.StringType);
//
//        datasetRow.select(mode.apply(col("vs"))).show();

//        datasetRow.map((MapFunction<Row, Row>) row -> {
//
//            Row study = (Row) row.getList(row.fieldIndex("studies")).get(0);
//            List<List<String>> samplesData = RowToVariantConverter.getSamplesData(study);
//
////            samplesData.get()
//
//            return row;
//        }, null);

//
//        StructType schema = datasetRow.schema();
//        schema.schema.getFieldIndex("studies").

//        UserDefinedFunction stats = udf(new VariantStatsFunction(), STATS_MAP_DATA_TYPE);
        UserDefinedFunction statsFromStudy = udf(new VariantStatsFromStudiesFunction(studyId, getCohort(), sampleIdx),
                DataTypes.createArrayType(VariantToRowConverter.STUDY_DATA_TYPE, false));

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
        //        List<Integer> samplePositions;
        private final VariantToRowConverter converter = new VariantToRowConverter();

        public VariantStatsFromStudiesFunction(String studyId, String cohortName, Set<Integer> samplesIdx) {
            this.cohortName = cohortName;
            this.studyId = studyId;
            this.samplesIdx = samplesIdx;
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

//            List<List<String>> samplesData = RowToVariantConverter.getSamplesData(study);
            StudyEntry studyEntry = RowToAvroConverter.convert(
                    study,
                    VariantToRowConverter.STUDY_DATA_TYPE,
                    StudyEntry.getClassSchema());
            List<List<String>> samplesData = studyEntry.getSamplesData();

            Map<String, Integer> map = new HashMap<>();
            if (samplesIdx != null && !samplesIdx.isEmpty()) {
                for (Integer sampleIdx : samplesIdx) {
                    map.compute(samplesData.get(sampleIdx).get(0), (k, i) -> i == null ? 1 : i + 1);
                }
            } else {
                for (List<String> sampleData : samplesData) {
                    map.compute(sampleData.get(0), (k, i) -> i == null ? 1 : i + 1);
                }
            }
            Map<Genotype, Integer> gtMap = new HashMap<>();
            map.forEach((gtStr, count) -> {
                Genotype gt;
                if (gtStr.equals("?/?")) {
                    gt = new Genotype("./.", reference, alternate);
                } else {
                    gt = new Genotype(gtStr, reference, alternate);
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
