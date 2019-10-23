package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;
import org.opencb.oskar.analysis.variant.tdt.TdtAnalysis;
import org.opencb.oskar.core.exceptions.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.transformers.TdtTransformer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;


public class TdtSparkParquetAnalysis extends TdtAnalysis implements SparkParquetAnalysis {

    public TdtSparkParquetAnalysis() {
    }

    public TdtSparkParquetAnalysis(String phenotype, ObjectMap executorParams, Path outDir) {
        super(phenotype, executorParams, outDir);
    }

    @Override
    public void exec() throws OskarAnalysisException {
        String parquetFilename = getFile();
        String studyId = getStudy();
        SparkSession sparkSession = getSparkSession("tdt");

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new OskarAnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        TdtTransformer tdtTransformer = new TdtTransformer()
                .setStudyId(studyId)
                .setPhenotype(getPhenotype());

        Dataset<Row> outputDataset = tdtTransformer.transform(inputDastaset);

        // Sanity check
        if (outputDataset == null) {
            throw new OskarAnalysisException("Something wrong happened! Output dataset is null when executing TDT analysis");
        }

        String outFilename = getOutDir() + "/tdt.txt";
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(getHeaderLine());

            // IMPORTANT: be careful with indices of each field in the row,
            Iterator<Row> rowIterator = outputDataset.withColumn("ct", explode(col("annotation.consequenceTypes")))
                    .selectExpr("chromosome", "start", "end", "strand", "reference", "alternate",
                            "annotation.id as dbSNP",
                            "ct.ensemblGeneId as ensemblGeneId",
                            "ct.biotype as biotype",
                            "ct.sequenceOntologyTerms.name as SO",
                            "chiSquare", "pValue", "oddRatio", "freedomDegrees", "t1", "t2").toLocalIterator();

            SparkAnalysisUtils.writeRows(rowIterator, pw);

            pw.close();
        } catch (FileNotFoundException e) {
            throw new OskarAnalysisException("Error saving TDT results", e);
        }
    }
}
