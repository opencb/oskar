package org.opencb.oskar.spark.variant.analysis;

import htsjdk.tribble.util.popgen.HardyWeinbergCalculation;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.opencb.oskar.spark.variant.udf.StudyFunction;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;


/**
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HardyWeinbergTransformer extends Transformer {

    private String uid;
    private Param<String> studyIdParam;

    public HardyWeinbergTransformer() {
        this(null);
    }

    public HardyWeinbergTransformer(String uid) {
        this.uid = uid;
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public String uid() {
        return getUid();
    }

    private String getUid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID("VariantStatsTransformer");
        }
        return uid;
    }

    public Param<String> studyIdParam() {
        return studyIdParam = studyIdParam == null ? new Param<>(this, "studyId", "") : studyIdParam;
    }

    public HardyWeinbergTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        UserDefinedFunction hardyWeinberg = udf(new HardyWeinbergFunction(getStudyId()), DataTypes.DoubleType);

        return dataset.withColumn("hw", hardyWeinberg.apply(new ListBuffer<Column>()
                .$plus$eq(col("studies"))
        ));
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
                switch (sampleData.apply(0)) {
                    case "0/0":
                    case "0|0":
                        obsAA++;
                        break;
                    case "0/1":
                    case "0|1":
                    case "1|0":
                        obsAB++;
                        break;
                    case "1/1":
                    case "1|1":
                        obsBB++;
                        break;
                    default:
                        break;
                }
            }

            return HardyWeinbergCalculation.hwCalculate(obsAA, obsAB, obsBB);
        }
    }

}
