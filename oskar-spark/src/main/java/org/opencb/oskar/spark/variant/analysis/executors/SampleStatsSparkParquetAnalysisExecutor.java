package org.opencb.oskar.spark.variant.analysis.executors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.variant.stats.VariantSampleStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.analysis.variant.stats.SampleStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.SampleStatsExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantSampleStatsTransformer;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = SampleStatsAnalysis.ID,
        source= AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class SampleStatsSparkParquetAnalysisExecutor extends SampleStatsExecutor {

    public SampleStatsSparkParquetAnalysisExecutor() {
    }

    public SampleStatsSparkParquetAnalysisExecutor(List<String> sampleNames, ObjectMap executorParams, Path outDir) {
        super(sampleNames, executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
        String parquetFilename = getExecutorParams().getString("FILE");
        String studyId = getExecutorParams().getString("STUDY_ID");
        String master = getExecutorParams().getString("MASTER");

        // Prepare input dataset from the input parquet file
        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("sample stats")
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
        VariantSampleStatsTransformer transformer = new VariantSampleStatsTransformer();


        Dataset<Row> outputDs = transformer.setSamples(sampleNames).transform(inputDastaset);
        Map<String, VariantSampleStats> stats = VariantSampleStatsTransformer.toSampleStats(outputDs);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        String outFilename = getOutDir() + "/sample_stats.json";
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(objectMapper.writer().writeValueAsString(stats));
            pw.close();
        } catch (Exception e) {
            throw new AnalysisException("Error writing output file: " + outFilename, e);
        }

        if (new File(outFilename).exists()) {
            arm.addFile(Paths.get(outFilename), FileResult.FileType.JSON);
        }
    }
}
