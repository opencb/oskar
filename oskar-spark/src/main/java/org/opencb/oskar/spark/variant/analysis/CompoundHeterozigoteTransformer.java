package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.oskar.spark.variant.VariantMetadataManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.genes;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.sample_data_field;

/**
 * Created on 07/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CompoundHeterozigoteTransformer extends AbstractTransformer {

    private final Param<String> studyIdParam;
    private final Param<String> fatherParam;
    private final Param<String> motherParam;
    private final Param<String> childParam;
    private final Param<Boolean> missingGenotypeAsReferenceParam;

    public CompoundHeterozigoteTransformer() {
        this(null);
    }

    public CompoundHeterozigoteTransformer(String uid) {
        super(uid);
        studyIdParam = new Param<>(this, "studyId", "");
        fatherParam = new Param<>(this, "father", "");
        motherParam = new Param<>(this, "mother", "");
        childParam = new Param<>(this, "child", "");
        missingGenotypeAsReferenceParam = new Param<>(this, "missingGenotypeAsReference", "");

        setDefault(studyIdParam, "");
        setDefault(missingGenotypeAsReferenceParam, false);
    }

    public Param<String> studyIdParam() {
        return studyIdParam;
    }

    public CompoundHeterozigoteTransformer setStudyId(String studyId) {
        set(studyIdParam(), studyId);
        return this;
    }

    public String getStudyId() {
        return getOrDefault(studyIdParam());
    }

    public Param<String> fatherParam() {
        return fatherParam;
    }

    public CompoundHeterozigoteTransformer setFather(String father) {
        set(fatherParam, father);
        return this;
    }

    public String getFather() {
        return getOrDefault(fatherParam);
    }

    public Param<String> motherParam() {
        return motherParam;
    }

    public CompoundHeterozigoteTransformer setMother(String mother) {
        set(motherParam, mother);
        return this;
    }

    public String getMother() {
        return getOrDefault(motherParam);
    }

    public Param<String> childParam() {
        return childParam;
    }

    public CompoundHeterozigoteTransformer setChild(String child) {
        set(childParam, child);
        return this;
    }

    public String getChild() {
        return getOrDefault(childParam);
    }

    public Param<Boolean> missingGenotypeAsReferenceParam() {
        return missingGenotypeAsReferenceParam;
    }

    public CompoundHeterozigoteTransformer setMissingGenotypeAsReference(boolean missingGenotypeAsReference) {
        set(missingGenotypeAsReferenceParam, missingGenotypeAsReference);
        return this;
    }

    public Boolean getMissingGenotypeAsReference() {
        return getOrDefault(missingGenotypeAsReferenceParam);
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        CompoundHeterozigoteUDAF udaf = new CompoundHeterozigoteUDAF(getMissingGenotypeAsReference());

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

        if (samplesMap.size() == 1) {
            int fatherIdx = samples.indexOf(father);
            int motherIdx = samples.indexOf(mother);
            int childIdx = samples.indexOf(child);


            df = df.select(
                    explode(genes("annotation")).as("gene"),
                    concat(col("chromosome"), lit(":"), col("start"), lit(":"), col("reference"), lit(":"), col("alternate")).as("id"),
                    col("studies").getItem(0).getField("samplesData").getItem(fatherIdx).getItem(0).as("father"),
                    col("studies").getItem(0).getField("samplesData").getItem(motherIdx).getItem(0).as("mother"),
                    col("studies").getItem(0).getField("samplesData").getItem(childIdx).getItem(0).as("child"));
        } else {
            df = df.select(
                    explode(genes("annotation")).as("gene"),
                    concat(col("chromosome"), lit(":"), col("start"), lit(":"), col("reference"), lit(":"), col("alternate")).as("id"),
                    sample_data_field("studies", father, "GT").as("father"),
                    sample_data_field("studies", mother, "GT").as("mother"),
                    sample_data_field("studies", child, "GT").as("child"));
        }
        return df.filter(col("gene").notEqual(""))
                .groupBy("gene")
                .agg(udaf.apply(
                        col("gene"),
                        col("id"),
                        col("father"),
                        col("mother"),
                        col("child")).alias("r"))
                .selectExpr("r.*")
                .select(
                        col("gene"),
                        size(col("variants")).as("numVariants"),
                        explode(col("variants")).alias("variant"));
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return createStructType(new StructField[]{
                createStructField("gene", StringType, false),
                createStructField("numVariants", IntegerType, false),
                createStructField("variant", StringType, false),
        });
    }

    public static class CompoundHeterozigoteUDAF extends UserDefinedAggregateFunction {

        private boolean missingAsReference;

        public CompoundHeterozigoteUDAF(boolean missingAsReference) {
            this.missingAsReference = missingAsReference;
        }

        @Override
        public StructType inputSchema() {
            return createStructType(new StructField[]{
                    createStructField("gene", StringType, false),
                    createStructField("id", StringType, false),
                    createStructField("childGt", StringType, false),
                    createStructField("fatherGt", StringType, false),
                    createStructField("motherGt", StringType, false),
            });
        }

        @Override
        public StructType bufferSchema() {
            return createStructType(new StructField[]{
                    createStructField("gene", StringType, false),
                    createStructField("fatherExplainedVariants", createArrayType(StringType, false), false),
                    createStructField("motherExplainedVariants", createArrayType(StringType, false), false),
            });
        }

        @Override
        public StructType dataType() {
            return createStructType(new StructField[]{
                    createStructField("gene", StringType, false),
                    createStructField("variants", createArrayType(StringType, false), false),
            });
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, "");
            buffer.update(1, Collections.emptyList());
            buffer.update(2, Collections.emptyList());
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            // Update gene
            buffer.update(0, input.get(0));
            String variantId = input.getString(1);
            String fatherGt = getGt(input, 2);
            String motherGt = getGt(input, 3);
            String childGt = getGt(input, 4);
            int v = ModeOfInheritance.compoundHeterozygosityVariantExplainType(childGt, fatherGt, motherGt);
            if (v == 1) {
                addToList(buffer, variantId, 1);
            } else if (v == 2) {
                addToList(buffer, variantId, 2);
            }
        }

        private void addToList(MutableAggregationBuffer buffer, String variantId, int i) {
            List<Object> list = new ArrayList<>(buffer.getList(i));
            list.add(variantId);
            buffer.update(i, list);
        }

        private String getGt(Row input, int i) {
            String gt = input.getString(i);
            if (missingAsReference && gt.equals("./.")) {
                return "0/0";
            }
            return gt;
        }

        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0, buffer2.get(0));
            List<Object> list = new ArrayList<>(buffer1.getList(1));
            list.addAll(buffer2.getList(1));
            buffer1.update(1, list);
            list = new ArrayList<>(buffer1.getList(2));
            list.addAll(buffer2.getList(2));
            buffer1.update(2, list);
        }

        @Override
        public Object evaluate(Row buffer) {
            List<String> fatherExplainedVariantList = buffer.getList(1);
            List<String> motherExplainedVariantList = buffer.getList(2);
            List<String> variantList;
            if (!fatherExplainedVariantList.isEmpty() && !motherExplainedVariantList.isEmpty()) {
                variantList = new ArrayList<>(fatherExplainedVariantList.size() + motherExplainedVariantList.size());
                variantList.addAll(fatherExplainedVariantList);
                variantList.addAll(motherExplainedVariantList);
            } else {
                variantList = Collections.emptyList();
            }
            return new GenericRowWithSchema(new Object[]{
                    buffer.get(0),
                    variantList,
            }, dataType());
        }
    }

}
