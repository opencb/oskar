package org.opencb.oskar.spark.variant.converters;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantStats;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.tools.Converter;
import org.opencb.oskar.spark.commons.converters.AvroToRowConverter;

import java.io.Serializable;

import static org.opencb.oskar.spark.commons.converters.DataTypeUtils.getFieldIdx;

/**
 * Created on 12/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToRowConverter extends AvroToRowConverter implements Converter<Variant, Row>, Serializable {

    public static final StructType VARIANT_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(VariantAvro.getClassSchema()).dataType());
    public static final StructType STATS_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(VariantStats.getClassSchema()).dataType());
    public static final StructType STUDY_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(StudyEntry.getClassSchema()).dataType());
    public static final StructType FILE_DATA_TYPE = ((StructType) SchemaConverters.toSqlType(FileEntry.getClassSchema()).dataType());
    public static final StructType ANNOTATION_DATA_TYPE = ((StructType) VARIANT_DATA_TYPE.apply("annotation").dataType());

    public static final int STUDY_ID_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "studyId");
    public static final int SAMPLES_DATA_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "samplesData");
    public static final int FORMAT_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "format");
    public static final int FILES_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "files");
    public static final int FILE_ID_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "files.fileId");
    public static final int FILE_ATTRIBUTES_IDX =
            getFieldIdx(STUDY_DATA_TYPE, "files.attributes");


    public static final int CONSEQUENCE_TYPES_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes");
    public static final int CONSEQUENCE_TYPES_GENE_NAME_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.geneName");
    public static final int CONSEQUENCE_TYPES_ENSEMBL_GENE_ID_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.ensemblGeneId");
    public static final int CONSEQUENCE_TYPES_ENSEMBL_TRANSCRIPT_ID_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.ensemblTranscriptId");
    public static final int BIOTYPE_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.biotype");
    public static final int SEQUENCE_ONTOLOGY_TERM_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.sequenceOntologyTerms");
    public static final int SEQUENCE_ONTOLOGY_TERM_NAME_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.sequenceOntologyTerms.name");
    public static final int PROTEIN_VARIANT_ANNOTATION_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.proteinVariantAnnotation");
    public static final int SUBSTITUTION_SCORES_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.proteinVariantAnnotation.substitutionScores");

    public static final int SCORE_SCORE_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.proteinVariantAnnotation.substitutionScores.score");
    public static final int SCORE_SOURCE_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.proteinVariantAnnotation.substitutionScores.source");
public static final int SCORE_DESCRIPTION_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "consequenceTypes.proteinVariantAnnotation.substitutionScores.description");

    public static final int POPULATION_FREQUENCIES_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "populationFrequencies");
    public static final int POPULATION_FREQUENCIES_STUDY_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "populationFrequencies.study");
    public static final int POPULATION_FREQUENCIES_POPULATION_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "populationFrequencies.population");
    public static final int POPULATION_FREQUENCIES_ALT_ALLELE_FREQ_IDX =
            getFieldIdx(ANNOTATION_DATA_TYPE, "populationFrequencies.altAlleleFreq");

    @Override
    public Row convert(Variant variant) {
        return convert(variant.getImpl());
    }

    public Row convert(VariantAvro variantAvro) {
        return (Row) convert(variantAvro, VARIANT_DATA_TYPE);
    }

    public Row convert(StudyEntry studyEntry) {
        return (Row) convert(studyEntry, STUDY_DATA_TYPE);
    }

    public Row convert(VariantStats stats) {
        return (Row) convert(stats, STATS_DATA_TYPE);
    }

    public Row convert(VariantSetStats stats) {
        return (Row) convert(stats, SchemaConverters.toSqlType(VariantSetStats.getClassSchema()).dataType());
    }

}
