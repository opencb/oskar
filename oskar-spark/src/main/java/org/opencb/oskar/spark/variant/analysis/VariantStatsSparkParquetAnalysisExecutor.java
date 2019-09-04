package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.stats.AbstractVariantStatsExecutor;
import org.opencb.oskar.analysis.variant.stats.SampleStats;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.VariantStatsTransformer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class VariantStatsSparkParquetAnalysisExecutor extends AbstractVariantStatsExecutor {

    public VariantStatsSparkParquetAnalysisExecutor() {
    }

    public VariantStatsSparkParquetAnalysisExecutor(String cohort, ObjectMap executorParams, Path outDir) {
        super(cohort, executorParams, outDir);
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
                .appName("sample stats")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new AnalysisException("Error loading Parquet file: " + parquetFilename, e);
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

        String outFilename = getOutDir() + "/variant_stats.txt";
        PrintWriter pw;
        try {
            pw = new PrintWriter(outFilename);
        } catch (FileNotFoundException e) {
            throw new AnalysisException("Error creating output file: " + outFilename, e);
        }
        pw.println(line);

        SparkAnalysisExecutorUtils.writeRows(outDf.toLocalIterator(), pw);

        pw.close();

        List<AnalysisResult.File> resultFiles = new ArrayList<>();
        if (new File(outFilename).exists()) {
            resultFiles.add(new AnalysisResult.File(Paths.get(outFilename), AnalysisResult.FileType.TAB_SEPARATED));
        }

        return new AnalysisResult()
                .setAnalysisId(SampleStats.ID)
                .setDateTime(getDateTime())
                .setExecutorParams(executorParams)
                .setOutputFiles(resultFiles)
                .setExecutionTime(watch.getTime());
    }
}
