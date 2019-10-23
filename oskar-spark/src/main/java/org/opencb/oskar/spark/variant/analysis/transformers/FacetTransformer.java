package org.opencb.oskar.spark.variant.analysis.transformers;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.spark.variant.converters.DataframeToFacetFieldConverter;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.regex.Matcher;

import static org.apache.spark.sql.functions.*;
import static org.opencb.oskar.spark.variant.converters.DataframeToFacetFieldConverter.*;
import static org.opencb.oskar.spark.variant.udf.VariantUdfManager.*;

public class FacetTransformer extends AbstractTransformer {

    public static final String SEPARATOR = "__";
    public static final String POPFREQ_PREFIX = "popFreq" + SEPARATOR;
    public static final String STATS_PREFIX = "stats" + SEPARATOR;

    private Param<String> facetParam;

    private Map<String, String> validCategoricalFields;
    private Map<String, String> validRangeFields;
    private Set<String> isExplode;

    private DataframeToFacetFieldConverter converter;

    public FacetTransformer() {
        this(null);
    }

    public FacetTransformer(String uid) {
        super(uid);
        converter = new DataframeToFacetFieldConverter();
        facetParam = new Param<>(this, "facet", "");

        setDefault(facetParam(), null);

        init();

    }

    // Study ID parameter
    public Param<String> facetParam() {
        return facetParam;
    }

    public FacetTransformer setFacet(String facetParam) {
        set(facetParam(), facetParam);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> df) {
        Dataset<Row> res = (Dataset<Row>) df;


        // Sanity check
        String facet = getOrDefault(facetParam());
        if (StringUtils.isEmpty(facet)) {
            return df.sparkSession().emptyDataFrame();
        }

        if (facet.contains(converter.NESTED_FACET_SEPARATOR)) {
            // Nested facet
            String[] facets = facet.split(converter.NESTED_FACET_SEPARATOR);

            // Sanity check
            for (int i = 0; i < facets.length - 1; i++) {
                if (converter.getFacetType(facets[i]) == FacetType.AGGREGATION) {
                    throw new InvalidParameterException("In nested facets, aggregations must be in last place: " + facet);
                }
            }
            List<String> fieldNames = new LinkedList<>();
            List<String> facetNames = new LinkedList<>();
            for (String simpleFacet: facets) {
                String fieldName = converter.getFieldName(simpleFacet);
                if (fieldNames.contains(fieldName)) {
                    throw new InvalidParameterException("In nested facets, repeating facets are not allowed: " + facet);
                }
                fieldNames.add(fieldName);
            }

            // Process facet
            boolean aggregation = false;
            res = (Dataset<Row>) df;
            for (int i = 0; i < fieldNames.size(); i++) {
                switch (getFacetType(facets[i])) {
                    case CATEGORICAL: {
                        // Categorical facet
                        String facetName = fieldNames.get(i);
                        res = processCategoricalFacet(facets[i], fieldNames.get(i), facetName, res);
                        facetNames.add(facetName);
                        break;
                    }
                    case RANGE: {
                        // Range facet
                        String facetName = (fieldNames.get(i) + "Range");
                        res = processRangeFacet(facets[i], fieldNames.get(i), facetName, res);
                        facetNames.add(facetName);
                        break;
                    }
                    case AGGREGATION: {
                        // Aggregation must be the last one!!!
                        res = processAggregationFacet(facets[i], fieldNames.get(i), res);
                        aggregation = true;
                        break;
                    }
                    default: {
                        throw new InvalidParameterException("In nested facets, unknown facet in middle position: " + facet);
                    }
                }
            }

            Column[] cols = new Column[facetNames.size()];
            for (int i = 0; i < facetNames.size(); i++) {
                cols[i] = new Column(facetNames.get(i));
            }
            if (aggregation) {
                // Special case, we have aggregations
                int index = facets.length - 1;
                String aggFunct = facets[index].substring(0, facets[index].indexOf("("));
                if (fieldNames.get(index).startsWith(STATS_PREFIX)) {
                    String tmpName = fieldNames.get(index).replace(":", "___").replace("@", "____");
                    res = res.withColumnRenamed(fieldNames.get(index), tmpName).groupBy(cols)
                            .agg(getAggregationExpr(aggFunct, tmpName).as(fieldNames.get(index)),
                                    count(lit(1)).as("count"))
                            .orderBy(cols);
                } else {
                    res = res.groupBy(cols).agg(getAggregationExpr(aggFunct, fieldNames.get(index)),
                            count(lit(1)).as("count")).orderBy(cols);
                }



            } else {
                res = res.groupBy(cols).count().orderBy(cols);
            }
        } else {
            // Simple facet: categorical, range or aggregation
            String fieldName = getFieldName(facet);
            switch (getFacetType(facet)) {
                case CATEGORICAL: {
                    // Categorical facet
                    String facetName = fieldName;
                    Dataset<Row> categoricalDf = processCategoricalFacet(facet, fieldName, facetName, (Dataset<Row>) df);
                    res = categoricalDf.groupBy(facetName).count().orderBy(facetName);
                    break;
                }
                case RANGE: {
                    // Range facet
                    String facetName = (fieldName + "Range");
                    Dataset<Row> rangeDf = processRangeFacet(facet, fieldName, facetName, (Dataset<Row>) df);
                    res = rangeDf.groupBy(facetName).count().orderBy(facetName);
                    break;
                }
                case AGGREGATION: {
                    // Aggregation facet
                    Dataset<Row> cached = processAggregationFacet(facet, fieldName, (Dataset<Row>) df);
                    long count = cached.count();
                    String aggFunct = facet.substring(0, facet.indexOf("("));
                    if (fieldName.startsWith(STATS_PREFIX)) {
                        String tmpName = fieldName.replace(":", "___").replace("@", "____");
                        res = cached.withColumnRenamed(fieldName, tmpName).agg(getAggregationExpr(aggFunct, tmpName))
                                .withColumn("count", lit(count))
                                .withColumnRenamed(aggFunct + "(" + tmpName + ")", aggFunct + "(" + fieldName + ")");
                    } else {
                        res = cached.agg(getAggregationExpr(aggFunct, fieldName)).withColumn("count", lit(count));
                    }
                    break;
                }
                default:
                    throw new InvalidParameterException("Unknown facet in middle position: " + facet);
            }
        }

        // Save facet in metadata
        Metadata facetMetadata = new MetadataBuilder().putString("facet", facet).build();
        return res.withColumn("count", col("count").as("count", facetMetadata));
    }

    private Column getAggregationExpr(String aggFunct, String fieldName) {
        String label = aggFunct + "(" + fieldName + ")";
        if (aggFunct.equals("sumsq")) {
            return expr("sum(power(" + fieldName + ", 2))").as(label);
        } else if (aggFunct.equals("percentile")) {
            return expr("percentile(" + fieldName + ", array(" + PERCENTILE_PARAMS + "))").as(label);
        } else if (aggFunct.equals("unique")) {
            return expr("collect_set(" + fieldName + ")").as(label);
        } else {
            return expr(label);
        }

    }

    private Dataset<Row> processCategoricalFacet(String facet, String fieldName, String facetName, Dataset<Row> df) {
        Dataset<Row> res = df;
        if (isValidField(fieldName)) {
            if (isNumeric(fieldName)) {
                if (fieldName.startsWith(POPFREQ_PREFIX)) {
                    String[] splits = fieldName.split(SEPARATOR);
                    res = res.withColumn(facetName, population_frequency("annotation", splits[1], splits[2]));
                } else if (fieldName.startsWith(STATS_PREFIX)) {
                    String[] splits = fieldName.split(SEPARATOR);
                    res = res.withColumn("tmp", study("studies", splits[1]))
                            .withColumn(facetName, col("tmp.stats." + splits[2] + ".altAlleleFreq"));
                } else {
                    UserDefinedFunction scoreFunction = udf(new ScoreFunction(fieldName), DataTypes.DoubleType);
                    if (isSubstitutionScore(fieldName)) {
                        res = res.withColumn("tmp1", getColumn(fieldName));
                    }
                    ListBuffer<Column> functScoreSeq = createFunctScoreSeq(fieldName, "tmp1");
                    res = res.withColumn(facetName, scoreFunction.apply(functScoreSeq));
                }
            } else if (isExplode.contains(fieldName)) {
                res = res.withColumn(facetName, getColumn(fieldName));
            }
            List<String> values = getIncludeValues(facet);
            if (ListUtils.isNotEmpty(values)) {
                StringBuilder include = new StringBuilder(facetName).append("='").append(values.get(0)).append("'");
                for (int i = 1; i < values.size(); i++) {
                    include.append(" OR ").append(facetName).append("='").append(values.get(i)).append("'");
                }
                res = res.filter(include.toString());
            }
        }
        return res;
    }

    private Dataset<Row> processRangeFacet(String facet, String fieldName, String facetName, Dataset<Row> df) {
        // Parse range
        String[] split = facet.substring(facet.indexOf("[") + 1).replace("[", ":").replace("..", ":").replace("]", "").split(":");
        double start = Double.parseDouble(split[0]);
        double end = Double.parseDouble(split[1]);
        double step = Double.parseDouble(split[2]);

        Column col;
        String alias = null;
        String tmpName = facetName;
        if (facet.startsWith(POPFREQ_PREFIX)) {
            String[] splits = fieldName.split(SEPARATOR);
            col = population_frequency("annotation", splits[1], splits[2]);
        } else if (facet.startsWith(STATS_PREFIX)) {
            alias = facetName;
            tmpName = facetName.replace(":", "___").replace("@", "____");

            String[] splits = fieldName.split(SEPARATOR);
            df = df.withColumn("tmp", study("studies", splits[1]));
            col = col("tmp.stats." + splits[2] + ".altAlleleFreq");
        } else {
            UserDefinedFunction scoreFunction = udf(new ScoreFunction(fieldName), DataTypes.DoubleType);
            if (isSubstitutionScore(fieldName)) {
                df = df.withColumn("tmp1", getColumn(fieldName));
            }
            ListBuffer<Column> functScoreSeq = createFunctScoreSeq(fieldName, "tmp1");
            col = scoreFunction.apply(functScoreSeq);
        }

        df = df.withColumn(tmpName, col.divide(step).cast(DataTypes.IntegerType).multiply(step))
                .filter(tmpName + ">= " + start + " AND " + tmpName + " <= " + end);

        if (alias != null) {
            df = df.withColumnRenamed(tmpName, alias);
        }
        return df;
    }

    private Dataset<Row> processAggregationFacet(String facet, String fieldName, Dataset<Row> df) {
        // Validate aggregation function
        String aggFunction = facet.substring(0, facet.indexOf("("));
        boolean found = false;
        for (String agg: DataframeToFacetFieldConverter.AGGREGATION_FUNCTIONS) {
            if (agg.equals(aggFunction)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new InvalidParameterException("Aggregation function unknown: " + aggFunction);
        }

        if (isNumeric(fieldName)) {
            if (validRangeFields.containsKey(fieldName)) {
                UserDefinedFunction scoreFunction = udf(new ScoreFunction(fieldName), DataTypes.DoubleType);
                if (isSubstitutionScore(fieldName)) {
                    df = df.withColumn("tmp1", getColumn(fieldName));
                }
                ListBuffer<Column> functScoreSeq = createFunctScoreSeq(fieldName, "tmp1");
                return df.withColumn(fieldName, scoreFunction.apply(functScoreSeq));
            } else if (fieldName.startsWith(POPFREQ_PREFIX)) {
                String[] splits = fieldName.split(SEPARATOR);
                return df.withColumn(fieldName, population_frequency("annotation", splits[1], splits[2]));
            } else if (fieldName.startsWith(STATS_PREFIX)) {
                String[] splits = fieldName.split(SEPARATOR);
                return df.withColumn("tmp", study("studies", splits[1]))
                        .withColumn(fieldName, col("tmp.stats." + splits[2] + ".altAlleleFreq"));
            }
        }
        return df;
    }

    private ListBuffer<Column> createFunctScoreSeq(String fieldName) {
        return createFunctScoreSeq(fieldName, null);
    }

    private ListBuffer<Column> createFunctScoreSeq(String fieldName, String aux) {
        if (isFunctionalScore(fieldName)) {
            return new ListBuffer<Column>().$plus$eq(col("annotation.functionalScore"));
        } else if (isSubstitutionScore(fieldName)) {
            //return new ListBuffer<Column>().$plus$eq(col("annotation.consequenceTypes.proteinVariantAnnotation.substitutionScores"));
            return new ListBuffer<Column>().$plus$eq(col(aux));
        } else if (isConservationScore(fieldName)) {
            return new ListBuffer<Column>().$plus$eq(col("annotation.conservation"));
        } else {
            return null;
        }
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
//        List<StructField> fields = new ArrayList<>();
//        fields.add(createStructField("id", DoubleType, false));
//        fields.add(createStructField("type", DoubleType, false));
//        fields.add(createStructField("phastCons", DoubleType, false));
//        fields.add(createStructField("phylop", DoubleType, false));
//        fields.add(createStructField("gerp", DoubleType, false));
//        fields.add(createStructField("caddRaw", DoubleType, false));
//        fields.add(createStructField("caddScaled", DoubleType, false));
//        return createStructType(fields);
    }

    public static class ScoreFunction extends AbstractFunction1<WrappedArray<GenericRowWithSchema>,
            Double> implements Serializable {
        private String source;

        public ScoreFunction(String source) {
            this.source = source;
        }

        @Override
        public Double apply(WrappedArray<GenericRowWithSchema> functionalScores) {
            if (functionalScores == null) {
                return Double.NEGATIVE_INFINITY;
            }
            for (int i = 0; i < functionalScores.length(); i++) {
                Row functScore = functionalScores.apply(i);
                if (functScore.apply(1).equals(source)) {
                    return Double.parseDouble(functScore.apply(0).toString());
                }
            }

            return Double.NEGATIVE_INFINITY;
        }
    }

    private void init() {
        // Categorical fields
        validCategoricalFields = new HashMap<>();
        validCategoricalFields.put("chromosome", "chromosome");
        validCategoricalFields.put("type", "type");
        validCategoricalFields.put("studies", "studies.studyId");
        validCategoricalFields.put("biotype", "annotation.consequenceTypes.biotype");
        validCategoricalFields.put("ct", "annotation.consequenceTypes.sequenceOntologyTerms.name");
        validCategoricalFields.put("gene", "annotation.consequenceTypes.geneName");
        validCategoricalFields.put("ensemblGeneId", "annotation.consequenceTypes.ensemblGeneId");
        validCategoricalFields.put("ensemblTranscriptId", "annotation.consequenceTypes.ensemblTranscriptId");
        validCategoricalFields.put("gerp", "annotation.conservation");
        validCategoricalFields.put("phylop", "annotation.conservation");
        validCategoricalFields.put("phastCons", "annotation.conservation");
        validCategoricalFields.put("cadd_scaled", "annotation.functionalScore");
        validCategoricalFields.put("cadd_raw", "annotation.functionalScore");
        validCategoricalFields.put("sift", "annotation.consequenceTypes.proteinVariantAnnotation.substitutionScores");
        validCategoricalFields.put("polyphen", "annotation.consequenceTypes.proteinVariantAnnotation.substitutionScores");

        // Is explode?
        isExplode = new HashSet<>();
        isExplode.add("studies");
        isExplode.add("biotype");
        isExplode.add("gene");
        isExplode.add("ensemblGeneId");
        isExplode.add("ensemblTranscriptId");
        isExplode.add("ct");
        isExplode.add("gerp");
        isExplode.add("phylop");
        isExplode.add("phastCons");
        isExplode.add("cadd_scaled");
        isExplode.add("cadd_raw");
        isExplode.add("sift");
        isExplode.add("polyphen");


        // Range fields
        validRangeFields = new HashMap<>();
        validRangeFields.put("gerp", "annotation.conservation");
        validRangeFields.put("phylop", "annotation.conservation");
        validRangeFields.put("phastCons", "annotation.conservation");
        validRangeFields.put("cadd_scaled", "annotation.functionalScore");
        validRangeFields.put("cadd_raw", "annotation.functionalScore");
        validRangeFields.put("sift", "annotation.consequenceTypes.proteinVariantAnnotation.substitutionScores");
        validRangeFields.put("polyphen", "annotation.consequenceTypes.proteinVariantAnnotation.substitutionScores");
    }

    private boolean isValidField(String field) {
        if (validCategoricalFields.containsKey(field) || isNumeric(field)) {
            return true;
        }
        return false;
    }

    private boolean isNumeric(String field) {
        if (validRangeFields.containsKey(field)
                || field.startsWith(POPFREQ_PREFIX) || field.startsWith(STATS_PREFIX)) {
            return true;
        }
        return false;
    }

    private boolean isFunctionalScore(String field) {
        return (field.equals("cadd_scaled") || field.equals("cadd_raw"));
    }

    private boolean isConservationScore(String field) {
        return (field.equals("gerp") || field.equals("phylop") || field.equals("phastCons"));
    }

    private boolean isSubstitutionScore(String field) {
        return (field.equals("sift") || field.equals("polyphen"));
    }

    private Column getColumn(String facetName) {
        if (isExplode.contains(facetName)) {
            switch(facetName) {
                case "gene":
                    return explode(genes("annotation"));
                case "biotype":
                    return explode(biotypes("annotation"));
                case "ct":
                    return explode(consequence_types("annotation"));
                default:
                    return explode(col(validCategoricalFields.get(facetName)));
            }
        } else {
            return col(validCategoricalFields.get(facetName));
        }
    }

    private List<String> getIncludeValues(String facet) {
        // Categorical...
        if (facet.contains("[")) {
            Matcher matcher = CATEGORICAL_PATTERN.matcher(facet);
            if (matcher.find()) {
                String include = matcher.group(2).replace("[", "").replace("]", "");

                if (StringUtils.isNotEmpty(include)) {
                    if (!include.contains("*")) {
                        return Arrays.asList(include.split(INCLUDE_SEPARATOR));
                    }
                }
            }
        }
        return new ArrayList<>();
    }
}
