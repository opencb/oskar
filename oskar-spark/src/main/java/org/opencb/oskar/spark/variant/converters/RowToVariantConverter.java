package org.opencb.oskar.spark.variant.converters;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.commons.Converter;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created on 08/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RowToVariantConverter implements MapFunction<Row, Variant>, Converter<Row, Variant> {

    @Override
    public Variant convert(Row row) {
        return call(row);
    }

    @Override
    public Variant call(Row row) {
//        SchemaConverters.SchemaType schemaType = SchemaConverters.toSqlType(VariantAvro.getClassSchema());
//        SchemaConverters.<VariantAvro>convertStructToAvro(((StructType) schemaType.dataType()), new SchemaBuilder.RecordBuilder<>(), "");
        VariantBuilder builder = new VariantBuilder();
        builder.setChromosome(row.getAs("chromosome"));
        builder.setId(row.getAs("id"));
        builder.setNames(row.getList(row.fieldIndex("names")));
        builder.setReference(row.getAs("reference"));
        builder.setAlternate(row.getAs("alternate"));
        builder.setStart(row.getAs("start"));
        builder.setEnd(row.getAs("end"));
        builder.setLength(row.getAs("length"));
        builder.setType(VariantType.valueOf(row.getAs("type")));
        List<Row> studies = row.getList(row.fieldIndex("studies"));
        Row study = studies.get(0);
        builder.setStudyId(study.getAs("studyId"));
        builder.setSampleDataKeys(study.getList(study.fieldIndex("format")));

        List<List<String>> samplesDataJava = getSamplesData(study);
//        builder.setSamplesData(samplesDataJava);

        return builder.build();
    }

    public static List<List<String>> getSamplesData(Row study) {
        List<WrappedArray<String>> samplesData = study.getList(study.fieldIndex("samplesData"));
        List<List<String>> samplesDataJava = new ArrayList<>(samplesData.size());
        for (WrappedArray<String> samplesDatum : samplesData) {
            if (samplesDatum.array() instanceof String[]) {
                String[] array = (String[]) samplesDatum.array();
                samplesDataJava.add(Arrays.asList(array));
            } else if (samplesDatum.array() instanceof Object[]) {
                Object[] array = (Object[]) samplesDatum.array();
                ArrayList<String> sampleData = new ArrayList<>(array.length);
                for (Object o : array) {
                    sampleData.add(o.toString());
                }
                samplesDataJava.add(sampleData);
            }
        }
        return samplesDataJava;
    }

}
