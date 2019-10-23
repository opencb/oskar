package org.opencb.oskar.spark.variant.transformers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.transformers.params.HasStudyId;
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
public class MendelianErrorTransformer extends AbstractTransformer implements HasStudyId {

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
        setDefault(studyIdParam(), "");
    }

    @Override
    public MendelianErrorTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
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
        Map<String, List<String>> samplesMap = new VariantMetadataManager().samples(df);
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

        return df.withColumn("mendelianError", mendelianUdf.apply(new ListBuffer<Column>()
                .$plus$eq(col("chromosome"))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(fatherIdx).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(motherIdx).getItem(0))
                .$plus$eq(col("studies").getItem(0).getField("samplesData").getItem(childIdx).getItem(0))));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        List<StructField> fields = Arrays.stream(schema.fields()).collect(Collectors.toList());
        fields.add(createStructField("mendelianError", IntegerType, false));
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

        @Override
        public Integer apply(String chromosome, String father, String mother, String child) {
            return MendelianError.compute(new Genotype(father), new Genotype(mother), new Genotype(child), chromosome);
        }
    }
}
