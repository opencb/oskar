package org.opencb.oskar.spark.variant.analysis;

import com.google.common.base.Throwables;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.oskar.analysis.variant.MendelianError;
import org.opencb.oskar.analysis.variant.TdtTest;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

public class TdtTransformer extends AbstractTransformer {
    private Param<String> studyIdParam;
    private Param<String> phenotypeParam;

    public TdtTransformer() {
        this(null);
    }

    public TdtTransformer(String uid) {
        super(uid);
        studyIdParam = new Param<>(this, "studyId", "");
        phenotypeParam = new Param<>(this, "phenotype", "");
    }

    // Study ID parameter
    public Param<String> studyIdParam() {
        return studyIdParam;
    }

    public TdtTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    // Phenotype parameter
    public Param<String> phenotypeParam() {
        return phenotypeParam;
    }

    public TdtTransformer setPhenotype(String phenotype) {
        set(phenotypeParam(), phenotype);
        return this;
    }

    public String getPhenotype() {
        return getOrDefault(phenotypeParam());
    }

    // Main function
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        try {
            List<Pedigree> pedigrees = new Oskar().pedigree(df, getStudyId());
            Phenotype phenotype = new Phenotype(getPhenotype(), getPhenotype(), "");
            List<String> sampleNames = new Oskar().samples(df, getStudyId());

            UserDefinedFunction tdt = udf(new TdtTransformer.TdtFunction(getStudyId(), pedigrees, phenotype,
                            sampleNames), DataTypes.DoubleType);

            return dataset.withColumn("TDT", tdt.apply(new ListBuffer<Column>().$plus$eq(col("studies"))));
        } catch (OskarException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField("TDT", DoubleType, false));
        return createStructType(fields);
    }

    public static class TdtFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>,
            Double> implements Serializable {
        private final String studyId;
        private final List<Pedigree> pedigrees;
        private final Phenotype phenotype;
        private final List<String> sampleNames;;

        public TdtFunction(String studyId, List<Pedigree> pedigrees, Phenotype phenotype, List<String> sampleNames) {
            this.studyId = studyId;
            this.pedigrees = pedigrees;
            this.phenotype = phenotype;
            this.sampleNames = sampleNames;
        }

        @Override
        public Double apply(WrappedArray<GenericRowWithSchema> studies) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

            // Prepare genotype map
            Map<String, Genotype> genotypes = new HashMap<>();
            List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
            for (int i = 0; i < sampleNames.size(); i++) {
                WrappedArray<String> sampleData = samplesData.get(i);
                genotypes.put(sampleNames.get(i), new Genotype(sampleData.apply(0)));
            }

            // Get chromosome
            String chromosome = study.getString(study.fieldIndex("chromosome"));

            return new TdtTest().computeTdtTest(genotypes, pedigrees, phenotype, chromosome).getpValue();
        }
    }
}
