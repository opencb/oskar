package org.opencb.oskar.spark.variant.analysis.executors;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.variant.gwas.Gwas;
import org.opencb.oskar.analysis.variant.gwas.GwasExecutor;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.ChiSquareTransformer;
import org.opencb.oskar.spark.variant.analysis.transformers.FisherTransformer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.CHI_SQUARE_TEST;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.FISHER_TEST;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = Gwas.ID,
        source= AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class GwasSparkParquetAnalysisExecutor extends GwasExecutor {

    public GwasSparkParquetAnalysisExecutor() {
    }

    public GwasSparkParquetAnalysisExecutor(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir,
                                            GwasConfiguration configuration) {
        super(list1, list2, executorParams, outDir, configuration);
    }

    public GwasSparkParquetAnalysisExecutor(String phenotype, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(phenotype, executorParams, outDir, configuration);
    }

    @Override
    public void exec() throws AnalysisException {

        String parquetFilename = getExecutorParams().getString("FILE");
        String studyId = getExecutorParams().getString("STUDY_ID");
        String master = getExecutorParams().getString("MASTER");

        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("GWAS")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        Oskar oskar = new Oskar(sparkSession);
        Dataset<Row> inputDastaset = null;
        try {
            inputDastaset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new AnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        if (getConfiguration().getMethod() == null) {
            throw new AnalysisException("Missing GWAS method. Valid methods are: " + CHI_SQUARE_TEST + " and " + FISHER_TEST);
        }

        switch (getConfiguration().getMethod()) {
            case CHI_SQUARE_TEST:
                chiSquare(inputDastaset, studyId);
                break;
            case FISHER_TEST:
                fisher(inputDastaset, studyId, FisherExactTest.TWO_SIDED);
                break;
            default:
                throw new AnalysisException("Unsupported GWAS method: " + getConfiguration().getMethod() + ". Supported methods are: "
                        + CHI_SQUARE_TEST + " and " + FISHER_TEST);
        }

        registerFiles();
    }

    private void fisher(Dataset<Row> inputDastaset, String studyId, int mode) throws AnalysisException {
        FisherTransformer fisherTransformer = new FisherTransformer();
        Dataset<Row> outputDataset;

        if (StringUtils.isNotEmpty(getPhenotype())) {
            outputDataset = fisherTransformer
                    .setStudyId(studyId)
                    .setPhenotype(getPhenotype())
                    .setMode(mode)
                    .transform(inputDastaset);
        } else if (CollectionUtils.isNotEmpty(getList1()) && CollectionUtils.isNotEmpty(getList2())) {
            outputDataset = fisherTransformer
                    .setStudyId(studyId)
                    .setSampleList1(getList1())
                    .setSampleList2(getList2())
                    .setMode(mode)
                    .transform(inputDastaset);
        } else {
            throw new AnalysisException("Invalid parameters when executing GWAS analysis (Fisher test)");
        }

        // Save
        save(outputDataset, FisherTransformer.FISHER_COL_NAME);
    }

    private void chiSquare(Dataset<Row> inputDastaset, String studyId) throws AnalysisException {
        ChiSquareTransformer chiSquareTransformer = new ChiSquareTransformer();
        Dataset<Row> outputDataset;

        if (StringUtils.isNotEmpty(getPhenotype())) {
            outputDataset = chiSquareTransformer.setStudyId(studyId)
                    .setPhenotype(getPhenotype())
                    .transform(inputDastaset);

        } else if (CollectionUtils.isNotEmpty(getList1()) && CollectionUtils.isNotEmpty(getList2())) {
            outputDataset = chiSquareTransformer.setStudyId(studyId)
                    .setSampleList1(getList1())
                    .setSampleList2(getList2())
                    .transform(inputDastaset);
        } else {
            throw new AnalysisException("Invalid parameters when executing GWAS analysis (chi square test)");
        }

        // Save
        save(outputDataset, ChiSquareTransformer.CHI_SQUARE_COL_NAME);
    }

    private void save(Dataset<Row> dataset, String colName) throws AnalysisException {
        // Sanity check
        if (dataset == null) {
            throw new AnalysisException("Something wrong happened! Output dataset is null when executing GWAS analysis");
        }

        String outFilename = getOutDir() + "/" + getOutputFilename();
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(getHeaderLine());

            // IMPORTANT: be careful with indices of each field in the row,
            Iterator<Row> rowIterator = dataset.withColumn("ct", explode(col("annotation.consequenceTypes")))
                    .selectExpr("chromosome", "start", "end", "strand", "reference", "alternate",
                            "annotation.id as dbSNP",
                            "ct.ensemblGeneId as ensemblGeneId",
                            "ct.biotype as biotype",
                            "ct.sequenceOntologyTerms.name as SO",
                            colName).toLocalIterator();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 9; i++) {
                    sb.append(row.get(i) == null ? "" : row.get(i)).append("\t");
                }
                List<String> soList = row.getList(9);
                if (CollectionUtils.isNotEmpty(soList)) {
                    sb.append(StringUtils.join(soList, ","));
                }
                List<Double> stats = row.getList(10);
                if (CollectionUtils.isNotEmpty(stats)) {
                    for (Double value : stats) {
                        sb.append("\t").append(value);
                    }
                }
                pw.println(sb);
            }
            pw.close();
        } catch (FileNotFoundException e) {
            throw new AnalysisException("Error saving GWAS results", e);
        }
    }
}
