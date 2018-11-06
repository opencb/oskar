package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.opencb.oskar.spark.variant.converters.VariantToRowConverter;

import static org.apache.spark.sql.functions.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.VariantUdf.*;

/**
 * Created on 12/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantUdfManager {

    enum VariantUdf {
        revcomp(new RevcompFunction(), DataTypes.StringType),

        study(new StudyFunction(), VariantToRowConverter.STUDY_DATA_TYPE),
        file(new FileFunction(), VariantToRowConverter.FILE_DATA_TYPE),
        fileAttribute(new FileAttributeFunction(), DataTypes.StringType),
        fileQual(new FileQualFunction(), DataTypes.DoubleType),
        fileFilter(new FileFilterFunction(), new ArrayType(DataTypes.StringType, false)),
        sampleData(new SampleDataFunction(), new ArrayType(DataTypes.StringType, false)),
        sampleDataField(new SampleDataFieldFunction(), DataTypes.StringType),

        genes(new GenesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequenceTypes(new ConsequenceTypesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequenceTypesByGene(new ConsequenceTypesByGeneFunction(), new ArrayType(DataTypes.StringType, false)),
        biotypes(new BiotypesFunction(), new ArrayType(DataTypes.StringType, false)),
        proteinSubstitution(new ProteinSubstitutionScoreFunction(), new ArrayType(DataTypes.DoubleType, false)),
        populationFrequencyAsMap(new PopulationFrequencyAsMapFunction(), new MapType(DataTypes.StringType, DataTypes.DoubleType, false)),
        populationFrequency(new PopulationFrequencyFunction(), DataTypes.DoubleType);

        private final DataType returnType;
        private final UserDefinedFunction udf;
        private final Class<?> udfClass;

        VariantUdf(Object function, DataType returnType) {
            udfClass = function.getClass();
            // With this UDF, there is no automatic input type coercion.
            this.udf = udf(function, returnType);
            this.returnType = returnType;
        }

        public DataType getReturnType() {
            return returnType;
        }

        public String getReturnTypeAsJson() {
            return returnType.json();
        }

        public UserDefinedFunction getUdf() {
            return udf;
        }

        public String getUdfClassName() {
            return udfClass.getName();
        }
    }

    /**
     * Load all Variant UserDefinedFunction.
     *
     * Variant UDFs are defined in the enum {@link VariantUdf}
     *
     * @param spark SparkSession
     */
    public void loadVariantUdfs(SparkSession spark) {
        for (VariantUdf udf : VariantUdf.values()) {
            spark.udf().register(udf.name(), udf.getUdf());
        }
    }

    public static Column revcomp(Column allele) {
        return callUDF(revcomp.name(), allele);
    }

    public static Column study(Column studiesColumn, String studyId) {
        return callUDF(study.name(), studiesColumn, lit(studyId));
    }

    public static Column file(Column study, String fileId) {
        return callUDF(file.name(), study, lit(fileId));
    }

    public static Column fileAttribute(Column studiesColumn, String file, String attributeField) {
        return callUDF(fileAttribute.name(), studiesColumn, lit(file), lit(attributeField));
    }

    public static Column fileFilter(Column studiesColumn, String file) {
        return callUDF(fileFilter.name(), studiesColumn, lit(file));
    }

    public static Column fileQual(Column studiesColumn, String file) {
        return callUDF(fileQual.name(), studiesColumn, lit(file));
    }

    public static Column sampleData(String studiesColumn, String sample) {
        return sampleData(col(studiesColumn), sample);
    }

    public static Column sampleData(Column studiesColumn, String sample) {
        return callUDF(sampleData.name(), studiesColumn, lit(sample));
    }

    public static Column sampleDataField(String studiesColumn, String sample, String formatFiel) {
        return sampleDataField(col(studiesColumn), sample, formatFiel);
    }

    public static Column sampleDataField(Column studiesColumn, String sample, String formatField) {
        return callUDF(sampleDataField.name(), studiesColumn, lit(sample), lit(formatField));
    }

    public static Column genes(String annotation) {
        return genes(col(annotation));
    }

    public static Column genes(Column annotation) {
        return callUDF(genes.name(), annotation);
    }

    public static Column consequenceTypes(String annotation) {
        return consequenceTypes(col(annotation));
    }

    public static Column consequenceTypes(Column annotation) {
        return callUDF(consequenceTypes.name(), annotation);
    }

    public static Column consequenceTypesByGene(Column annotation, String gene) {
        return callUDF(consequenceTypesByGene.name(), annotation, lit(gene));
    }

    public static Column proteinSubstitution(Column annotation, String source) {
        return proteinSubstitution(annotation, lit(source));
    }

    public static Column biotypes(String annotation) {
        return biotypes(col(annotation));
    }

    public static Column biotypes(Column annotation) {
        return callUDF(biotypes.name(), annotation);
    }

    public static Column proteinSubstitution(Column annotation, Column source) {
        return callUDF(proteinSubstitution.name(), annotation, source);
    }

    public static Column populationFrequencyAsMap(Column annotation) {
        return callUDF(populationFrequencyAsMap.name(), annotation);
    }

    public static Column populationFrequency(String annotation, String study, String population) {
        return populationFrequency(col(annotation), study, population);
    }

    public static Column populationFrequency(Column annotation, String study, String population) {
        return callUDF(populationFrequency.name(), annotation, lit(study), lit(population));
    }

//    public static Column includeStudy(Column studiesColumn, String studies) {
//        UserDefinedFunction udf = udf(new IncludeStudyFunction(), DataTypes.createArrayType(VariantToRowConverter.STUDY_DATA_TYPE));
//
//        return udf.apply(new ListBuffer<Column>()
//                .$plus$eq(studiesColumn)
//                .$plus$eq(lit(studies)));
//    }

//    public static class ExcludeFunction
//            extends AbstractFunction2<GenericRowWithSchema, String, GenericRowWithSchema>
//            implements UDF2<GenericRowWithSchema, String, GenericRowWithSchema> {
//        @Override
//        public GenericRowWithSchema call(GenericRowWithSchema row, String exclude) {
//            Object[] values = new Object[row.length()];
//
//            String[] excludeSplit = exclude.split(",");
//            Set<String> excludeFieldsSet = new HashSet<>(excludeSplit.length);
//            excludeFieldsSet.addAll(Arrays.asList(excludeSplit));
//
//            StructType schema = row.schema();
//            for (int i = 0; i < row.length(); i++) {
//                StructField field = schema.apply(i);
//                if (excludeFieldsSet.contains(field.name())) {
//                    // Exclude this field
//                    values[i] = null;
//                } else {
//                    values[i] = row.get(i);
//                }
//            }
//
//            return new GenericRowWithSchema(values, schema);
//        }
//
//        @Override
//        public GenericRowWithSchema apply(GenericRowWithSchema row, String include) {
//            return call(row, include);
//        }
//    }

}
