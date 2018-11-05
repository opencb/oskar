package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction4;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

/**
 * Created on 12/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorTransformer extends Transformer {


    private String uid;
//    private Param<String> cohortParam;
    private Param<String> studyIdParam;

    public MendelianErrorTransformer() {
        this(null);
    }

    public MendelianErrorTransformer(String uid) {
        this.uid = uid;
//        setDefault(cohortParam(), "ALL");
        setDefault(studyIdParam(), "");
    }
//
//    public Param<String> cohortParam() {
//        return cohortParam = cohortParam == null ? new Param<>(this, "cohort", "") : cohortParam;
//    }
//
//    public MendelianErrorTransformer setCohort(String cohort) {
//        set(cohortParam, cohort);
//        return this;
//    }
//
//    public String getCohort() {
//        return getOrDefault(cohortParam);
//    }

    public Param<String> studyIdParam() {
        return studyIdParam = studyIdParam == null ? new Param<>(this, "studyId", "") : studyIdParam;
    }

    public MendelianErrorTransformer setStudyId(String studyId) {
        set(studyIdParam, studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam);
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

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {


        Dataset<Row> datasetRow = (Dataset<Row>) dataset;
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
        UserDefinedFunction statsFromStudy = udf(new MendelianErrorFunction(), DataTypes.IntegerType);

        return datasetRow.withColumn("code", statsFromStudy.apply(new ListBuffer<Column>()
                .$plus$eq(col("chromosome"))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(0).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(1).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(2).getItem(0))
        ));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    public static class MendelianErrorFunction
            extends AbstractFunction4<String, String, String, String, Integer> implements Serializable {

        private enum GenotypeCode {
            HOM_REF, HOM_VAR, HET
        }

        @Override
        public Integer apply(String chromosome, String father, String mother, String child) {
            Genotype fatherGt = new Genotype(father);
            Genotype motherGt = new Genotype(mother);
            Genotype childGt = new Genotype(child);
            final int code;

            if (fatherGt.getCode() != AllelesCode.ALLELES_MISSING
                    && motherGt.getCode() != AllelesCode.ALLELES_MISSING
                    && childGt.getCode() != AllelesCode.ALLELES_MISSING) {
                GenotypeCode fatherCode = getAlternateAlleleCount(fatherGt);
                GenotypeCode motherCode = getAlternateAlleleCount(motherGt);
                GenotypeCode childCode = getAlternateAlleleCount(childGt);

                if (chromosome.equals("X")) {
                    code = 0;
                } else if (chromosome.equals("Y")) {
                    code = 0;
                } else {
                    if (childCode == GenotypeCode.HET) {
                        if (fatherCode == GenotypeCode.HOM_VAR && motherCode == GenotypeCode.HOM_VAR) {
                            code = 1;
                        } else if (fatherCode == GenotypeCode.HOM_REF && motherCode == GenotypeCode.HOM_REF) {
                            code = 2;
                        } else {
                            code = 0;
                        }
                    } else if (childCode == GenotypeCode.HOM_VAR) {
                        if (fatherCode == GenotypeCode.HOM_REF && motherCode != GenotypeCode.HOM_REF) {
                            code = 3;
                        } else if (fatherCode != GenotypeCode.HOM_REF && motherCode == GenotypeCode.HOM_REF) {
                            code = 4;
                        } else if (fatherCode == GenotypeCode.HOM_REF && motherCode == GenotypeCode.HOM_REF) {
                            code = 5;
                        } else {
                            code = 0;
                        }
                    } else if (childCode == GenotypeCode.HOM_REF) {
                        if (fatherCode == GenotypeCode.HOM_VAR && motherCode != GenotypeCode.HOM_VAR) {
                            code = 6;
                        } else if (fatherCode != GenotypeCode.HOM_VAR && motherCode == GenotypeCode.HOM_VAR) {
                            code = 7;
                        } else if (fatherCode == GenotypeCode.HOM_VAR && motherCode == GenotypeCode.HOM_VAR) {
                            code = 8;
                        } else {
                            code = 0;
                        }
                    } else {
                        code = 0;
                    }
                }
            } else {
                code = 0;
            }

            return code;
        }

        private GenotypeCode getAlternateAlleleCount(Genotype gt) {
            int count = 0;
            for (int i : gt.getAllelesIdx()) {
                if (i > 0) {
                    count++;
                }
            }
            switch (count) {
                case 0:
                    return GenotypeCode.HOM_REF;
                case 1:
                    return GenotypeCode.HET;
                default:
                    return GenotypeCode.HOM_REF;
            }
        }
    }
}
