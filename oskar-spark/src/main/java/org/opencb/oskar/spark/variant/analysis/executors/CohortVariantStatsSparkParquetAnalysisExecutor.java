package org.opencb.oskar.spark.variant.analysis.executors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.CohortVariantStatsAnalysisExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantSetStatsTransformer;

import java.nio.file.Path;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = CohortVariantStatsAnalysis.ID,
        source= AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class CohortVariantStatsSparkParquetAnalysisExecutor extends CohortVariantStatsAnalysisExecutor {

    public CohortVariantStatsSparkParquetAnalysisExecutor() {
    }

    public CohortVariantStatsSparkParquetAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws AnalysisException {
        String parquetFilename = getExecutorParams().getString("FILE");
        String studyId = getStudy();
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

        writeStatsToFile(stats);
    }
}
