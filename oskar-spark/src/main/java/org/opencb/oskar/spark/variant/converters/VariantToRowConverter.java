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
    public static final StructType VARIANT_ANNOTATION_DATA_TYPE = ((StructType) VARIANT_DATA_TYPE.apply("annotation").dataType());

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
