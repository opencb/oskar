package org.opencb.oskar.spark.variant.analysis.executors;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.gwas.Gwas;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.analysis.variant.gwas.GwasExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.analysis.transformers.GwasTransformer;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.CHI_SQUARE_TEST;

@AnalysisExecutor(
        id = "spark-parquet",
        analysis = Gwas.ID,
        source = AnalysisExecutor.Source.PARQUET_FILE,
        framework = AnalysisExecutor.Framework.SPARK)
public class GwasSparkParquetAnalysisExecutor extends GwasExecutor {

    public GwasSparkParquetAnalysisExecutor() {
    }

    public GwasSparkParquetAnalysisExecutor(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(executorParams, outDir, configuration);
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
        Dataset<Row> inputDataset;
        try {
            inputDataset = oskar.load(parquetFilename);
        } catch (OskarException e) {
            throw new AnalysisException("Error loading Parquet file: " + parquetFilename, e);
        }

        GwasTransformer gwasTransformer = new GwasTransformer().setStudyId(studyId);

        // Set configuration
        gwasTransformer.setMethod(getConfiguration().getMethod().label);
        gwasTransformer.setFisherMode(getConfiguration().getFisherMode().label);

        // Set input
        if (CollectionUtils.isNotEmpty(getSampleList1()) || CollectionUtils.isNotEmpty(getSampleList2())) {
            gwasTransformer.setSampleList1(getSampleList1()).setSampleList2(getSampleList2());
        } else if (StringUtils.isNotEmpty(getPhenotype1()) || StringUtils.isNotEmpty(getPhenotype2())) {
            gwasTransformer.setPhenotype1(getPhenotype1()).setPhenotype2(getPhenotype2());
        } else if (StringUtils.isNotEmpty(getCohort1()) || StringUtils.isNotEmpty(getCohort2())) {
            gwasTransformer.setCohort1(getCohort1()).setCohort2(getCohort2());
        } else {
            throw new AnalysisException("Invalid parameters when executing GWAS analysis");
        }

        Dataset<Row> outputDataset = gwasTransformer.transform(inputDataset);

        // Sanity check
        if (outputDataset == null) {
            throw new AnalysisException("Something wrong happened! Output dataset is null when executing GWAS analysis");
        }

        String outFilename = getOutputFile().toString();
        try {
            PrintWriter pw = new PrintWriter(outFilename);
            pw.println(getHeaderLine());

            // IMPORTANT: be careful with indices of each field in the row,
            Iterator<Row> rowIterator = outputDataset.withColumn("ct", explode(col("annotation.consequenceTypes")))
                    .selectExpr(getColumnNames(getConfiguration().getMethod())).toLocalIterator();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < row.size(); i++) {
                    if (i == 9) {
                        // For consequence types
                        List<String> soList = row.getList(9);
                        if (CollectionUtils.isNotEmpty(soList)) {
                            sb.append(StringUtils.join(soList, ","));
                        }
                        sb.append("\t");
                    } else {
                        sb.append(row.get(i) == null ? "" : row.get(i)).append("\t");
                    }
                }
                pw.println(sb);
            }
            pw.close();
        } catch (FileNotFoundException e) {
            throw new AnalysisException("Error saving GWAS results", e);
        }
    }

    private Seq<String> getColumnNames(GwasConfiguration.Method method) {
        List<String> colNames = new ArrayList<>();
        colNames.addAll(Arrays.asList(("chromosome,start,end,reference,alternate,annotation.id as dbSNP,"
                + "ct.ensemblGeneId as ensemblGeneId,ct.biotype as biotype,ct.sequenceOntologyTerms.name as SO")
                .split(",")));
        if (method == CHI_SQUARE_TEST) {
            colNames.add("chiSquare");
        }
        colNames.add("pValue");
        colNames.add("oddRatio");

        return JavaConversions.asScalaIterator(colNames.iterator()).toSeq();
    }
}
