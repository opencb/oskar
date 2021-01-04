package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.analysis.stats.TdtTest;
import org.opencb.oskar.analysis.stats.TdtTestResult;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.transformers.params.HasPhenotype;
import org.opencb.oskar.spark.variant.transformers.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

public class TdtTransformer extends AbstractTransformer implements HasStudyId, HasPhenotype {

    public static final String TDT_COL_NAME = "stats";

    public TdtTransformer() {
        this(null);
    }

    public TdtTransformer(String uid) {
        super(uid);
    }

    @Override
    public TdtTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    @Override
    public TdtTransformer setPhenotype(String phenotype) {
        set(phenotypeParam(), phenotype);
        return this;
    }

    // Main function

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        // Prepare families and affected samples from pedigree and phenotype name
        ObjectMap families = new ObjectMap();
        Set<String> affectedSamples = new HashSet<>();
        List<Pedigree> pedigrees = new Oskar().metadata().pedigrees(df, getStudyId());
        for (Pedigree pedigree: pedigrees) {
            ObjectMap family = new ObjectMap();
            for (Member member: pedigree.getMembers()) {
                ObjectMap sample = new ObjectMap();
                if (member.getFather() != null) {
                    sample.put("father", member.getFather().getId());
                }
                if (member.getMother() != null) {
                    sample.put("mother", member.getMother().getId());
                }
                if (member.getMultiples() != null && ListUtils.isNotEmpty(member.getMultiples().getSiblings())) {
                    sample.put("siblings", member.getMultiples().getSiblings());
                }
                // Add the sample to the family
                family.put(member.getId(), sample);

                // Is an affected member ?
                for (Phenotype phenotype: member.getPhenotypes()) {
                    if (getPhenotype().equals(phenotype.getId())) {
                        affectedSamples.add(member.getId());
                        break;
                    }
                }
            }
            // Add the family
            families.put(pedigree.getName(), family);
        }

        List<String> sampleNames = new Oskar().metadata().samples(df, getStudyId());

        UserDefinedFunction tdt = udf(new TdtTransformer.TdtFunction(getStudyId(), families, affectedSamples, sampleNames),
                DataTypes.createArrayType(DoubleType));

        String columnName = "stats";
        ListBuffer<Column> seq = new ListBuffer<Column>().$plus$eq(col("studies")).$plus$eq(col("chromosome"));
        Dataset<Row> ds = dataset.withColumn(columnName, tdt.apply(seq));
        return ds.withColumn("chiSquare", col(columnName).apply(0))
                .withColumn("pValue", col(columnName).apply(1))
                .withColumn("oddRatio", col(columnName).apply(2))
                .withColumn("freedomDegrees", col(columnName).apply(3))
                .withColumn("t1", col(columnName).apply(4))
                .withColumn("t2", col(columnName).apply(5))
                .drop(columnName);
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField(TDT_COL_NAME, createArrayType(DoubleType, false), false));
        return createStructType(fields);
    }

    public static class TdtFunction extends AbstractFunction2<WrappedArray<GenericRowWithSchema>, String, WrappedArray<Double>>
            implements Serializable {
        private final String studyId;
        private final ObjectMap families;
        private final Set<String> affectedSamples;
        private final List<String> sampleNames;

        public TdtFunction(String studyId, ObjectMap families, Set<String> affectedSamples, List<String> sampleNames) {
            this.studyId = studyId;
            this.families = families;
            this.affectedSamples = affectedSamples;
            this.sampleNames = sampleNames;
        }

        @Override
        public WrappedArray<Double> apply(WrappedArray<GenericRowWithSchema> studies, String chromosome) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

            // Prepare genotype map
            Map<String, String> genotypes = new HashMap<>();
            List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
            for (int i = 0; i < sampleNames.size(); i++) {
                WrappedArray<String> sampleData = samplesData.get(i);
                genotypes.put(sampleNames.get(i), sampleData.apply(0));
            }

            TdtTestResult tdtTestResult = new TdtTest().computeTdtTest(families, genotypes, affectedSamples, chromosome);

            double[] res = new double[6];
            res[0] = tdtTestResult.getChiSquare();
            res[1] = tdtTestResult.getpValue();
            res[2] = tdtTestResult.getOddRatio();
            res[3] = tdtTestResult.getDf();
            res[4] = tdtTestResult.getT1();
            res[5] = tdtTestResult.getT2();

            return WrappedArray.make(res);
        }
    }
}
