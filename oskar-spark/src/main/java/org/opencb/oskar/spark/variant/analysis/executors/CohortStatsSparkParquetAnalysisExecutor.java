package org.opencb.oskar.spark.variant.analysis.executors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.stats.AbstractCohortStatsExecutor;
import org.opencb.oskar.analysis.variant.stats.VariantStats;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantSetStatsTransformer;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CohortStatsSparkParquetAnalysisExecutor extends AbstractCohortStatsExecutor {

    public CohortStatsSparkParquetAnalysisExecutor() {
    }

    public CohortStatsSparkParquetAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public AnalysisResult exec() throws AnalysisException {
        StopWatch watch = StopWatch.createStarted();

        String parquetFilename = getExecutorParams().getString("FILE");
        String studyId = getExecutorParams().getString("STUDY_ID");
        String master = getExecutorParams().getString("MASTER");

        // Prepare input dataset from the input parquet file
        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("cohort stats")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset = null;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new AnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        // Call to the dataset transformer
        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer();

        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(inputDastaset).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        String outFilename = getOutDir() + "/cohort_stats.json";
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectMapper.writer().writeValueAsString(stats));
            pw.close();
        } catch (Exception e) {
            throw new AnalysisException("Error writing output file: " + outFilename, e);
        }

        List<AnalysisResult.File> resultFiles = new ArrayList<>();
        if (new File(outFilename).exists()) {
            resultFiles.add(new AnalysisResult.File(Paths.get(outFilename), AnalysisResult.FileType.JSON));
        }

        return new AnalysisResult()
                .setAnalysisId(VariantStats.ID)
                .setDateTime(getDateTime())
                .setExecutorParams(executorParams)
                .setOutputFiles(resultFiles)
                .setExecutionTime(watch.getTime());
    }
}
