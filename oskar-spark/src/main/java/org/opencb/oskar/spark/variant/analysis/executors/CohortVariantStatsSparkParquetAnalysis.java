package org.opencb.oskar.spark.variant.analysis.executors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;
import org.opencb.oskar.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.oskar.core.exceptions.OskarException;
import org.opencb.oskar.spark.commons.converters.RowToAvroConverter;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantSetStatsTransformer;

import java.nio.file.Path;


public class CohortVariantStatsSparkParquetAnalysis extends CohortVariantStatsAnalysis implements SparkParquetAnalysis {

    public CohortVariantStatsSparkParquetAnalysis() {
    }

    public CohortVariantStatsSparkParquetAnalysis(ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
    }

    @Override
    public void exec() throws OskarAnalysisException {
        String parquetFilename = getFile();
        String studyId = getStudy();
        SparkSession sparkSession = getSparkSession("cohort variant stats");

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset = null;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new OskarAnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        // Call to the dataset transformer
        VariantSetStatsTransformer transformer = new VariantSetStatsTransformer();

        GenericRowWithSchema result = (GenericRowWithSchema) transformer.transform(inputDastaset).collectAsList().get(0);
        VariantSetStats stats = RowToAvroConverter.convert(result, new VariantSetStats());

        writeStatsToFile(stats);
    }
}
