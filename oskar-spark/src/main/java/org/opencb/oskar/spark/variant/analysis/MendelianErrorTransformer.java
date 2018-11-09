package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.oskar.spark.variant.Oskar;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created on 12/06/18.
 *
 * Using Plink Mendel error codes
 * https://www.cog-genomics.org/plink2/basic_stats#mendel
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorTransformer extends AbstractTransformer {

    private Param<String> studyIdParam;
    private Param<String> fatherParam;
    private Param<String> motherParam;
    private Param<String> childParam;

    public MendelianErrorTransformer() {
        this(null);
    }

    public MendelianErrorTransformer(String uid) {
        super(uid);
        fatherParam = new Param<>(this, "father", "");
        motherParam = new Param<>(this, "mother", "");
        childParam = new Param<>(this, "child", "");
        studyIdParam = new Param<>(this, "studyId", "");
        setDefault(studyIdParam(), "");
    }

    public Param<String> studyIdParam() {
        return studyIdParam;
    }

    public MendelianErrorTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    public Param<String> fatherParam() {
        return fatherParam;
    }

    public MendelianErrorTransformer setFather(String father) {
        set(fatherParam, father);
        return this;
    }

    public String getFather() {
        return getOrDefault(fatherParam);
    }

    public Param<String> motherParam() {
        return motherParam;
    }

    public MendelianErrorTransformer setMother(String mother) {
        set(motherParam, mother);
        return this;
    }

    public String getMother() {
        return getOrDefault(motherParam);
    }

    public Param<String> childParam() {
        return childParam;
    }

    public MendelianErrorTransformer setChild(String child) {
        set(childParam, child);
        return this;
    }

    public String getChild() {
        return getOrDefault(childParam);
    }
    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        UserDefinedFunction mendelianUdf = udf(new MendelianErrorFunction("X", "Y"), IntegerType);

        List<String> samples;
        Map<String, List<String>> samplesMap = new Oskar().samples(df);
        // FIXME: Dataset can be multi-study
        if (StringUtils.isEmpty(getStudyId())) {
            samples = samplesMap.entrySet().iterator().next().getValue();
        } else {
            samples = samplesMap.get(getStudyId());
        }
        String father = getFather();
        String mother = getMother();
        String child = getChild();


        int fatherIdx = samples.indexOf(father);
        int motherIdx = samples.indexOf(mother);
        int childIdx = samples.indexOf(child);

        return df.withColumn("code", mendelianUdf.apply(new ListBuffer<Column>()
                .$plus$eq(col("chromosome"))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(fatherIdx).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(motherIdx).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(childIdx).getItem(0))
        ));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField("code", IntegerType, false));
        return createStructType(fields);
    }

    public static class MendelianErrorFunction
            extends AbstractFunction4<String, String, String, String, Integer> implements Serializable {

        private String chrX;
        private String chrY;

        public MendelianErrorFunction(String chrX, String chrY) {
            this.chrX = chrX;
            this.chrY = chrY;
        }

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

                if (chromosome.equals(chrX)) {
                    // TODO
                    code = 0;
                } else if (chromosome.equals(chrY)) {
                    // TODO
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
                    return GenotypeCode.HOM_VAR;
            }
        }
    }
}
