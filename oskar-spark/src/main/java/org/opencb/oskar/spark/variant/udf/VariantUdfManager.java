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
        file_attribute(new FileAttributeFunction(), DataTypes.StringType),
        file_qual(new FileQualFunction(), DataTypes.DoubleType),
        file_filter(new FileFilterFunction(), new ArrayType(DataTypes.StringType, false)),
        sample_data(new SampleDataFunction(), new ArrayType(DataTypes.StringType, false)),
        sample_data_field(new SampleDataFieldFunction(), DataTypes.StringType),

        genes(new GenesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequence_types(new ConsequenceTypesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequence_types_by_gene(new ConsequenceTypesByGeneFunction(), new ArrayType(DataTypes.StringType, false)),
        biotypes(new BiotypesFunction(), new ArrayType(DataTypes.StringType, false)),
        protein_substitution(new ProteinSubstitutionScoreFunction(), new ArrayType(DataTypes.DoubleType, false)),
        population_frequency_as_map(new PopulationFrequencyAsMapFunction(), new MapType(DataTypes.StringType, DataTypes.DoubleType, false)),
        population_frequency(new PopulationFrequencyFunction(), DataTypes.DoubleType);

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

    public static Column study(String studiesColumn, String studyId) {
        return study(col(studiesColumn), studyId);
    }

    public static Column file(Column study, String fileId) {
        return callUDF(file.name(), study, lit(fileId));
    }

    public static Column file(String study, String fileId) {
        return file(col(study), fileId);
    }

    public static Column file_attribute(Column studiesColumn, String file, String attributeField) {
        return callUDF(file_attribute.name(), studiesColumn, lit(file), lit(attributeField));
    }

    public static Column file_attribute(String studiesColumn, String file, String attributeField) {
        return file_attribute(col(studiesColumn), file, attributeField);
    }

    public static Column file_filter(Column studiesColumn, String file) {
        return callUDF(file_filter.name(), studiesColumn, lit(file));
    }

    public static Column file_filter(String studiesColumn, String file) {
        return file_filter(col(studiesColumn), file);
    }

    public static Column file_qual(Column studiesColumn, String file) {
        return callUDF(file_qual.name(), studiesColumn, lit(file));
    }

    public static Column file_qual(String studiesColumn, String file) {
        return file_qual(col(studiesColumn), file);
    }

    public static Column sample_data(String studiesColumn, String sample) {
        return sample_data(col(studiesColumn), sample);
    }

    public static Column sample_data(Column studiesColumn, String sample) {
        return callUDF(sample_data.name(), studiesColumn, lit(sample));
    }

    public static Column sample_data_field(String studiesColumn, String sample, String formatFiel) {
        return sample_data_field(col(studiesColumn), sample, formatFiel);
    }

    public static Column sample_data_field(Column studiesColumn, String sample, String formatField) {
        return callUDF(sample_data_field.name(), studiesColumn, lit(sample), lit(formatField));
    }

    public static Column genes(String annotation) {
        return genes(col(annotation));
    }

    public static Column genes(Column annotation) {
        return callUDF(genes.name(), annotation);
    }

    public static Column consequence_types(String annotation) {
        return consequence_types(col(annotation));
    }

    public static Column consequence_types(Column annotation) {
        return callUDF(consequence_types.name(), annotation);
    }

    public static Column consequence_types_by_gene(Column annotation, String gene) {
        return callUDF(consequence_types_by_gene.name(), annotation, lit(gene));
    }

    public static Column protein_substitution(Column annotation, String source) {
        return protein_substitution(annotation, lit(source));
    }

    public static Column protein_substitution(Column annotation, Column source) {
        return callUDF(protein_substitution.name(), annotation, source);
    }

    public static Column biotypes(String annotation) {
        return biotypes(col(annotation));
    }

    public static Column biotypes(Column annotation) {
        return callUDF(biotypes.name(), annotation);
    }

    public static Column population_frequency_as_map(String annotation) {
        return population_frequency_as_map(col(annotation));
    }

    public static Column population_frequency_as_map(Column annotation) {
        return callUDF(population_frequency_as_map.name(), annotation);
    }

    public static Column population_frequency(String annotation, String study, String population) {
        return population_frequency(col(annotation), study, population);
    }

    public static Column population_frequency(Column annotation, String study, String population) {
        return callUDF(population_frequency.name(), annotation, lit(study), lit(population));
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
