package org.opencb.oskar.spark.variant.converters;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.opencb.biodata.tools.Converter;
import org.opencb.commons.datastore.core.result.FacetQueryResult;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class DataframeToFacetFieldConverter implements Converter<Dataset<Row>, FacetQueryResult.Field> {

    public enum FacetType {
        CATEGORICAL,
        RANGE,
        AGGREGATION
    }

    public static final String FACET_SEPARATOR = ";";
    public static final String LABEL_SEPARATOR = "___";
    public static final String NESTED_FACET_SEPARATOR = ">>";
    private static final String NESTED_SUBFACET_SEPARATOR = ",";
    public static final String INCLUDE_SEPARATOR = ",";
    private static final String RANGE_IDENTIFIER = "..";
    private static final String AGGREGATION_IDENTIFIER = "(";
    public static final String[] AGGREGATION_FUNCTIONS = {"sum", "avg", "max", "min", "unique", "variance", "stddev",
            "unique", "percentile", "sumsq", };
    public static final Pattern CATEGORICAL_PATTERN = Pattern.compile("^([a-zA-Z][a-zA-Z0-9_.]+)(\\[[a-zA-Z0-9_.\\-,*]+])?(:\\*|:\\d+)?$");

    @Override
    public FacetQueryResult.Field convert(Dataset<Row> df) {
        // Sanity check
        String facet = df.schema().apply("count").metadata().getString("facet");

        String[] facets = facet.split(NESTED_FACET_SEPARATOR);
        FacetQueryResult.Field rootField = createFacetField(facets[0]).setBuckets(new ArrayList<>());

        // Main loop: iterating over rows
        Iterator<Row> rowIterator = df.toLocalIterator();
        while (rowIterator.hasNext()) {
            // Next row
            Row row = rowIterator.next();

            // Index to the "count" column (at last position)
            int countIdx = row.size() - 1;
            long count = row.getLong(countIdx);
            FacetQueryResult.Field field = rootField;
            for (int i = 0; i < countIdx; i++) {
                // Update field count
                field.setCount(field.getCount() + count);

                if (StringUtils.isEmpty(field.getAggregationName())) {
                    // Check if the bucket exists
                    String bucketValue = row.get(i).toString();
                    FacetQueryResult.Bucket bucket = getBucket(bucketValue, field.getBuckets());
                    if (bucket == null) {
                        // Create a new bucket with the
                        bucket = new FacetQueryResult.Bucket(bucketValue, 0, new ArrayList<>());
                        field.getBuckets().add(bucket);
                    }
                    // Update bucket count
                    bucket.setCount(bucket.getCount() + count);

                    // Nested facet ?
                    if (i + 1 < countIdx) {
                        // Nested facet, then create a new facet field inside the bucket
                        FacetQueryResult.Field newField = createFacetField(facets[i + 1]).setBuckets(new ArrayList<>());
                        bucket.getFields().add(newField);
                        field = newField;
                    }
                } else {
                    // Aggregation
                    // FIXME: support integer values, maybe AggregationValues should be of type Number
                    if (field.getAggregationName().equals("percentile")
                            || field.getAggregationName().equals("unique")) {
                        field.setAggregationValues(row.getList(i));
                    } else {
                        field.setAggregationValues(Collections.singletonList(Double.parseDouble("" + row.get(i))));
                    }

                    // Aggregation must be last one!!!
                    break;
                }
            }
        }
        return rootField;
    }

    private FacetQueryResult.Bucket getBucket(String value, List<FacetQueryResult.Bucket> buckets) {
        for (FacetQueryResult.Bucket bucket: buckets) {
            if (value.equals(bucket.getValue())) {
                return bucket;
            }
        }
        return null;
    }

    private FacetQueryResult.Field createFacetField(String facet) {
        // Categorical, range or aggregation facet
        String fieldName = getFieldName(facet);
        switch (getFacetType(facet)) {
            case CATEGORICAL: {
                // Categorical facet
                return new FacetQueryResult.Field(fieldName, "", new ArrayList<>());
            }
            case RANGE: {
                // Range facet
                String[] split = facet.replace("[", ":").replace("..", ":").replace("]", "").split(":");
                double start = Double.parseDouble(split[1]);
                double end = Double.parseDouble(split[2]);
                double step = Double.parseDouble(split[3]);

                return new FacetQueryResult.Field(fieldName, "", new ArrayList<>())
                        .setStart(start).setEnd(end).setStep(step);
            }
            case AGGREGATION: {
                // Aggregation facet
                return new FacetQueryResult.Field(fieldName, "", new ArrayList<>())
                        .setAggregationName(facet.substring(0, facet.indexOf("(")));
            }
            default:
                throw new InvalidParameterException("Unknown facet type: " + facet);
        }
    }

    public static String getFieldName(String facet) {
        if (facet.contains(AGGREGATION_IDENTIFIER)) {
            return facet.substring(facet.indexOf('(') + 1, facet.indexOf(')'));
        } else {
            if (facet.contains("[")) {
                return facet.substring(0, facet.indexOf('['));
            } else {
                return facet;
            }
        }
    }

    public static FacetType getFacetType(String facet) {
        if (facet.contains(RANGE_IDENTIFIER)) {
            return FacetType.RANGE;
        } else if (facet.contains(AGGREGATION_IDENTIFIER)) {
            return FacetType.AGGREGATION;
        } else {
            return FacetType.CATEGORICAL;
        }
    }
}
