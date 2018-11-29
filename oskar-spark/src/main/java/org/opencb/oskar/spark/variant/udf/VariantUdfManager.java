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
        genotype(new GenotypeFunction(), DataTypes.StringType),
        sample_data(new SampleDataFunction(), new ArrayType(DataTypes.StringType, false)),
        sample_data_field(new SampleDataFieldFunction(), DataTypes.StringType),

        genes(new GenesFunction(), new ArrayType(DataTypes.StringType, false)),
        ensembl_genes(new EnsemblGenesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequence_types(new ConsequenceTypesFunction(), new ArrayType(DataTypes.StringType, false)),
        consequence_types_by_gene(new ConsequenceTypesByGeneFunction(), new ArrayType(DataTypes.StringType, false)),
        biotypes(new BiotypesFunction(), new ArrayType(DataTypes.StringType, false)),
        protein_substitution(new ProteinSubstitutionScoreFunction(), new ArrayType(DataTypes.DoubleType, false)),
        functional(new FunctionalScoreFunction(), DataTypes.DoubleType),
        conservation(new ConservationScoreFunction(), DataTypes.DoubleType),
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
     * <p>
     * Variant UDFs are defined in the enum {@link VariantUdf}
     *
     * @param spark SparkSession
     */
    public void loadVariantUdfs(SparkSession spark) {
        for (VariantUdf udf : VariantUdf.values()) {
            spark.udf().register(udf.name(), udf.getUdf());
        }
    }

    /**
     * Get the Reverse and Complementary of the allele.
     *
     * <code>
     * df.select(revcomp(lit("AGG")).collect()
     * ["CCT"]
     * </code>
     *
     * @param allele Allele
     * @return Reverse complementary
     */
    public static Column revcomp(Column allele) {
        return callUDF(revcomp.name(), allele);
    }

    /**
     * Get the StudyEntry given a studyId.
     *
     * <code>
     * df.select(study("studies", "1000g").alias("1000g"))
     * </code>
     *
     * @param studies Studies column
     * @param studyId StudyId
     * @return StudyEntry as row
     */
    public static Column study(Column studies, String studyId) {
        return callUDF(study.name(), studies, lit(studyId));
    }

    /**
     * Get the StudyEntry given a studyId.
     *
     * <code>
     * df.select(study("studies", "1000g").alias("1000g"))
     * </code>
     *
     * @param studies Studies column
     * @param studyId StudyId
     * @return StudyEntry as row
     */
    public static Column study(String studies, String studyId) {
        return study(col(studies), studyId);
    }

    /**
     * Get the FileEntry given a fileId.
     *
     * <code>
     * df.select(file("studies", "file1"))
     * df.select(study("studies", "1000g").alias("1000g")).select(file("1000g", "file1"))
     * </code>
     *
     * @param study  List of studies or single study
     * @param fileId FileId
     * @return FileEntry as row
     */
    public static Column file(Column study, String fileId) {
        return callUDF(file.name(), study, lit(fileId));
    }

    /**
     * Get the FileEntry given a fileId.
     *
     * <code>
     * df.select(file("studies", "file1"))
     * df.select(study("studies", "1000g").alias("1000g")).select(file("1000g", "file1"))
     * </code>
     *
     * @param studies List of studies or single study
     * @param fileId  FileId
     * @return FileEntry as row
     */
    public static Column file(String studies, String fileId) {
        return file(col(studies), fileId);
    }

    /**
     * Get an attribute from a given a fileId.
     *
     * @param studies        List of studies or single study
     * @param fileId         FileId
     * @param attributeField File attribute
     * @return File attribute value as string
     */
    public static Column file_attribute(String studies, String fileId, String attributeField) {
        return file_attribute(col(studies), fileId, attributeField);
    }

    /**
     * Get an attribute from a given a fileId.
     *
     * @param studies        List of studies or single study
     * @param fileId         FileId
     * @param attributeField File attribute
     * @return File attribute value as string
     */
    public static Column file_attribute(Column studies, String fileId, String attributeField) {
        return callUDF(file_attribute.name(), studies, lit(fileId), lit(attributeField));
    }

    /**
     * Get the FILTER value from a given a fileId.
     *
     * @param studies List of studies or single study
     * @param fileId  FileId
     * @return File FILTER value as string
     */
    public static Column file_filter(Column studies, String fileId) {
        return callUDF(file_filter.name(), studies, lit(fileId));
    }

    /**
     * Get the FILTER value from a given a fileId.
     *
     * @param studies List of studies or single study
     * @param fileId  FileId
     * @return File FILTER value as string
     */
    public static Column file_filter(String studies, String fileId) {
        return file_filter(col(studies), fileId);
    }

    /**
     * Get the QUAL value from a given a fileId.
     *
     * @param studies List of studies or single study
     * @param fileId  FileId
     * @return File QUAL value as number
     */
    public static Column file_qual(String studies, String fileId) {
        return file_qual(col(studies), fileId);
    }

    /**
     * Get the QUAL value from a given a fileId.
     *
     * @param studies List of studies or single study
     * @param fileId  FileId
     * @return File QUAL value as number
     */
    public static Column file_qual(Column studies, String fileId) {
        return callUDF(file_qual.name(), studies, lit(fileId));
    }

    /**
     * Get the Genotype from a given sampleName.
     *
     * @param studies List of studies or single study
     * @param sample  Sample name
     * @return Genotype of the sample
     */
    public static Column genotype(String studies, String sample) {
        return genotype(col(studies), sample);
    }

    /**
     * Get the Genotype from a given sampleName.
     *
     * @param studies List of studies or single study
     * @param sample  Sample name
     * @return Genotype of the sample
     */
    public static Column genotype(Column studies, String sample) {
        return callUDF(genotype.name(), studies, lit(sample));
    }

    /**
     * Get the Sample Data from a given sampleName.
     *
     * @param studies List of studies or single study
     * @param sample  Sample name
     * @return List of sample data values
     */
    public static Column sample_data(String studies, String sample) {
        return sample_data(col(studies), sample);
    }

    /**
     * Get the Sample Data from a given sampleName.
     *
     * @param studies List of studies or single study
     * @param sample  Sample name
     * @return List of sample data values
     */
    public static Column sample_data(Column studies, String sample) {
        return callUDF(sample_data.name(), studies, lit(sample));
    }

    /**
     * Get the sample data value from a specific format field.
     *
     * <code>
     * df.select(sample_data_field("studies", "HG0096", "DP"))
     * df.select(split(sample_data_field("studies", "HG0096", "AD"), ","))
     * </code>
     *
     * @param studies     List of studies or single study
     * @param sample      Sample name
     * @param formatField Format field name
     * @return Value of the specific field from the sample data as String
     */
    public static Column sample_data_field(String studies, String sample, String formatField) {
        return sample_data_field(col(studies), sample, formatField);
    }

    /**
     * Get the sample data value from a specific format field.
     *
     * <code>
     * df.select(sample_data_field("studies", "HG0096", "DP"))
     * df.select(split(sample_data_field("studies", "HG0096", "AD"), ","))
     * </code>
     *
     * @param studies     List of studies or single study
     * @param sample      Sample name
     * @param formatField Format field name
     * @return Value of the specific field from the sample data as String
     */
    public static Column sample_data_field(Column studies, String sample, String formatField) {
        return callUDF(sample_data_field.name(), studies, lit(sample), lit(formatField));
    }

    /**
     * Get the list of HGNC gene names.
     *
     * @param annotation Annotation column
     * @return List of HGNC gene names
     */
    public static Column genes(String annotation) {
        return genes(col(annotation));
    }

    /**
     * Get the list of HGNC gene names.
     *
     * @param annotation Annotation column
     * @return List of HGNC gene names
     */
    public static Column genes(Column annotation) {
        return callUDF(genes.name(), annotation);
    }

    /**
     * Get the list of Ensembl gene ids.
     *
     * @param annotation Annotation column
     * @return List of Ensembl gene ids
     */
    public static Column ensembl_genes(String annotation) {
        return callUDF(ensembl_genes.name(), col(annotation));
    }

    /**
     * Get the list of Ensembl gene ids.
     *
     * @param annotation Annotation column
     * @return List of Ensembl gene ids
     */
    public static Column ensembl_genes(Column annotation) {
        return callUDF(ensembl_genes.name(), annotation);
    }

    /**
     * Get the list of Sequence Ontology terms.
     *
     * @param annotation Annotation column
     * @return List of sequence ontology terms.
     */
    public static Column consequence_types(String annotation) {
        return consequence_types(col(annotation));
    }

    /**
     * Get the list of Sequence Ontology terms.
     *
     * @param annotation Annotation column
     * @return List of sequence ontology terms.
     */
    public static Column consequence_types(Column annotation) {
        return callUDF(consequence_types.name(), annotation);
    }

    /**
     * Get the list of Sequence Ontology terms for a given gene name.
     *
     * @param annotation Annotation column
     * @param gene       HGNC or Ensembl gene name
     * @return List of sequence ontology terms.
     */
    public static Column consequence_types_by_gene(Column annotation, String gene) {
        return callUDF(consequence_types_by_gene.name(), annotation, lit(gene));
    }

    /**
     * Get the value for the Protein Substitution Score.
     * Because there are different values for each transcript, this function returns an array with the MIN and MAX values.
     *
     * @param annotation Annotation column
     * @param source     Protein substitution score source. Main sources: sift and polyphen
     * @return Array with MIN and MAX Protein substitution score values. Empty if none.
     */
    public static Column protein_substitution(String annotation, String source) {
        return callUDF(protein_substitution.name(), col(annotation), lit(source));
    }

    /**
     * Get the value for the Protein Substitution Score.
     * Because there are different values for each transcript, this function returns an array with the MIN and MAX values.
     *
     * @param annotation Annotation column
     * @param source     Protein substitution score source. Main sources: sift and polyphen
     * @return Array with MIN and MAX Protein substitution score values. Empty if none.
     */
    public static Column protein_substitution(Column annotation, String source) {
        return callUDF(protein_substitution.name(), annotation, lit(source));
    }

    /**
     * Get the value for the Conservation Score.
     *
     * @param annotation Annotation column
     * @param source     Conservation score source. Main sources: gerp, phastCons and phylop
     * @return Conservation score value. Null if none.
     */
    public static Column conservation(String annotation, String source) {
        return conservation(col(annotation), source);
    }

    /**
     * Get the value for the Conservation Score.
     *
     * @param annotation Annotation column
     * @param source     Conservation score source. Main sources: gerp, phastCons and phylop
     * @return Conservation score value. Null if none.
     */
    public static Column conservation(Column annotation, String source) {
        return callUDF(conservation.name(), annotation, lit(source));
    }

    /**
     * Get the value for the Functional Score.
     *
     * @param annotation Annotation column
     * @param source     Functional score source. Main sources: cadd_scaled and cadd_raw
     * @return Functional score value. Null if none.
     */
    public static Column functional(String annotation, String source) {
        return callUDF(functional.name(), col(annotation), lit(source));
    }

    /**
     * Get the value for the Functional Score.
     *
     * @param annotation Annotation column
     * @param source     Functional score source. Main sources: cadd_scaled and cadd_raw
     * @return Functional score value. Null if none.
     */
    public static Column functional(Column annotation, String source) {
        return callUDF(functional.name(), annotation, lit(source));
    }

    /**
     * Get the list of biotype terms.
     *
     * @param annotation Annotation column
     * @return List of biotyppe terms.
     */
    public static Column biotypes(String annotation) {
        return biotypes(col(annotation));
    }

    /**
     * Get the list of biotype terms.
     *
     * @param annotation Annotation column
     * @return List of biotyppe terms.
     */
    public static Column biotypes(Column annotation) {
        return callUDF(biotypes.name(), annotation);
    }

    /**
     * Get a map with all the population frequencies.
     * - Key:   study:population
     * - Value: Alternate Allele Frequency
     *
     * @param annotation Annotation column
     * @return Map with population frequencies.
     */
    public static Column population_frequency_as_map(String annotation) {
        return population_frequency_as_map(col(annotation));
    }

    /**
     * Get a map with all the population frequencies.
     * - Key:   study:population
     * - Value: Alternate Allele Frequency
     *
     * @param annotation Annotation column
     * @return Map with population frequencies.
     */
    public static Column population_frequency_as_map(Column annotation) {
        return callUDF(population_frequency_as_map.name(), annotation);
    }

    /**
     * Get the Alternate Allele Frequency of a given study and popylation.
     *
     * @param annotation Annotation column
     * @param study      Study
     * @param population Population
     * @return Map with population frequencies.
     */
    public static Column population_frequency(String annotation, String study, String population) {
        return population_frequency(col(annotation), study, population);
    }

    /**
     * Get the Alternate Allele Frequency of a given study and popylation.
     *
     * @param annotation Annotation column
     * @param study      Study
     * @param population Population
     * @return Map with population frequencies.
     */
    public static Column population_frequency(Column annotation, String study, String population) {
        return callUDF(population_frequency.name(), annotation, lit(study), lit(population));
    }

}
