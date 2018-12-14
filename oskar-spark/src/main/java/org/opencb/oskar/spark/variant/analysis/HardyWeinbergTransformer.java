package org.opencb.oskar.spark.variant.analysis;

import htsjdk.tribble.util.popgen.HardyWeinbergCalculation;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.oskar.spark.variant.analysis.params.HasStudyId;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;


/**
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HardyWeinbergTransformer extends AbstractTransformer implements HasStudyId {

    public HardyWeinbergTransformer() {
        this(null);
    }

    public HardyWeinbergTransformer(String uid) {
        super(uid);
    }

    @Override
    public HardyWeinbergTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        UserDefinedFunction hardyWeinberg = udf(new HardyWeinbergFunction(getStudyId()), DataTypes.DoubleType);

        return dataset.withColumn("HWE", hardyWeinberg.apply(new ListBuffer<Column>()
                .$plus$eq(col("studies"))));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField("HWE", DoubleType, false));
        return createStructType(fields);
    }

    public static class HardyWeinbergFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>, Double>
            implements Serializable {
        private final String studyId;

        public HardyWeinbergFunction(String studyId) {
            this.studyId = studyId;
        }

        @Override
        public Double apply(WrappedArray<GenericRowWithSchema> studies) {
            GenericRowWithSchema study = (GenericRowWithSchema) new StudyFunction().apply(studies, studyId);

            int obsAA = 0;
            int obsAB = 0;
            int obsBB = 0;

            List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
            for (WrappedArray<String> sampleData : samplesData) {
                MendelianError.GenotypeCode gtCode = MendelianError.getAlternateAlleleCount(new Genotype(sampleData.apply(0)));
                switch (gtCode) {
                    case HOM_REF:
                        obsAA++;
                        break;
                    case HET:
                        obsAB++;
                        break;
                    case HOM_VAR:
                        obsBB++;
                        break;
                    default:
                        break;
                }
            }

            // This class calculates a HardyWeinberg p-value given three values representing the observed frequences
            // of homozygous and heterozygous genotypes in the test population
            return HardyWeinbergCalculation.hwCalculate(obsAA, obsAB, obsBB);
        }
    }
}
