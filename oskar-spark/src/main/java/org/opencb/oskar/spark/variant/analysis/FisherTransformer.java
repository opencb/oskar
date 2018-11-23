package org.opencb.oskar.spark.variant.analysis;

import com.google.common.base.Throwables;
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
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.analysis.variant.FisherExactTest;
import org.opencb.oskar.analysis.variant.MendelianError;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.params.HasPhenotype;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

public class FisherTransformer extends AbstractTransformer implements HasStudyId, HasPhenotype {

    public FisherTransformer() {
        this(null);
    }

    public FisherTransformer(String uid) {
        super(uid);
    }

    @Override
    public FisherTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    @Override
    public FisherTransformer setPhenotype(String phenotype) {
        set(phenotypeParam(), phenotype);
        return this;
    }

    // Main function
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        // Search affected samples (and take the index)
        Set<Integer> affectedIndexSet = new HashSet<>();
        try {
            List<String> samples = new Oskar().metadata().samples(df, getStudyId());
            List<Pedigree> pedigrees = new Oskar().metadata().pedigrees(df, getStudyId());
            int i = 0;
            for (Pedigree pedigree: pedigrees) {
                for (Member member: pedigree.getMembers()) {
                    if (ListUtils.isNotEmpty(member.getPhenotypes())
                            && member.getPhenotypes().contains(getPhenotype())) {
                        affectedIndexSet.add(samples.indexOf(member.getId()));
                    }
                }
            }
        } catch (OskarException e) {
            throw Throwables.propagate(e);
        }

        UserDefinedFunction fisher = udf(new FisherTransformer.FisherFunction(getStudyId(), affectedIndexSet),
                DataTypes.DoubleType);

        return dataset.withColumn("Fisher p-value", fisher.apply(new ListBuffer<Column>().$plus$eq(col("studies"))));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField("Fisher p-value", DoubleType, false));
        return createStructType(fields);
    }

    public static class FisherFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>,
            Double> implements Serializable {
        private final String studyId;
        private final Set<Integer> affectedIndexSet;

        public FisherFunction(String studyId, Set<Integer> affectedIndexSet) {
            this.studyId = studyId;
            this.affectedIndexSet = affectedIndexSet;
        }

        @Override
        public Double apply(WrappedArray<GenericRowWithSchema> studies) {
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
                        } else {
                            b += 2;
                        }
                        break;
                    }
                    case HET: {
                        if (affectedIndexSet.contains(i)) {
                            a++;
                            c++;
                        } else {
                            b++;
                            d++;
                        }
                        break;
                    }
                    case HOM_VAR:
                        if (affectedIndexSet.contains(i)) {
                            c += 2;
                        } else {
                            d += 2;
                        }
                        break;
                    default:
                        break;
                }
            }

            return new FisherExactTest().fisherTest(a, b, c, d).getpValue();
        }
    }
}
