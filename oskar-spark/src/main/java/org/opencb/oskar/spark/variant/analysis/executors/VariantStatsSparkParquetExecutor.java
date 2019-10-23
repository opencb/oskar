package org.opencb.oskar.spark.variant.analysis.executors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.ExecutionException;
import org.opencb.oskar.analysis.variant.stats.VariantStatsExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantStatsTransformer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;


public class VariantStatsSparkParquetExecutor extends VariantStatsExecutor implements SparkParquetExecutor {

    public VariantStatsSparkParquetExecutor() {
    }

    public VariantStatsSparkParquetExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        super(cohort, executorParams, outDir);
    }

    @Override
    public void exec() throws ExecutionException {
        String parquetFilename = getFile();
        String studyId = getStudy();
        SparkSession sparkSession = getSparkSession("variant stats");

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new ExecutionException("Error loading Parquet file: " + parquetFilename, e);
        }

        // Call to the transformer dataset
        VariantStatsTransformer transformer = new VariantStatsTransformer().setCohort(getCohort());

        Dataset<Row> outDf = transformer.transform(inputDastaset).withColumn("ct", explode(col("annotation.consequenceTypes")))
                .select(col("chromosome"), col("start"), col("end"), col("strand"), col("reference"), col("alternate"),
                        col("annotation.id").as("dbSNP"), col("ct"),
                        col("studies").getItem(0).getField("stats").getField(getCohort()).as("cohort"))
                .selectExpr("chromosome", "start", "end", "strand", "reference", "alternate", "dbSNP", "ct.ensemblGeneId", "ct.biotype",
                        "ct.sequenceOntologyTerms.name as consequenceType",
                        "cohort.*");

        StringBuilder line = new StringBuilder("#");
        for (StructField field : outDf.schema().fields()) {
            if (line.length() != 1) {
                line.append("\t");
            }
            line.append(field.name());
        }

        Path outFilename = getOutputFile();
        PrintWriter pw;
        try {
            pw = new PrintWriter(outFilename.toFile());
        } catch (FileNotFoundException e) {
            throw new ExecutionException("Error creating output file: " + outFilename, e);
        }
        pw.println(line);

        SparkAnalysisExecutorUtils.writeRows(outDf.toLocalIterator(), pw);

        pw.close();
    }

}
