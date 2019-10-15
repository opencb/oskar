package org.opencb.oskar.analysis.variant.gwas;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.FisherMode.*;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.CHI_SQUARE_TEST;
import static org.opencb.oskar.analysis.variant.gwas.GwasConfiguration.Method.FISHER_TEST;

@Analysis(id = Gwas.ID, data = Analysis.AnalysisData.VARIANT)
public class Gwas extends OskarAnalysis {

    public static final String ID = "GWAS";

    private String study;
    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype1;
    private String phenotype2;
    private String cohort1;
    private String cohort2;
    private GwasConfiguration configuration;

    public Gwas() {
    }

    public Gwas(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(executorParams, outDir);
        this.configuration = configuration;
    }

    /**
     * Checks if sampleList1 and sampleList2 are not empty and no common samples exist.
     */
    @Override
    protected void check() throws AnalysisException {
        // Set configuration
        GwasConfiguration.Method method = getConfiguration().getMethod();
        GwasConfiguration.FisherMode fisherMode = getConfiguration().getFisherMode();
        if (method == null) {
            throw new AnalysisException("Missing GWAS method. Valid methods are: " + CHI_SQUARE_TEST.label + " and " + FISHER_TEST.label);
        }
        if (method != FISHER_TEST && method != CHI_SQUARE_TEST) {
            throw new AnalysisException("Invalid GWAS method: " + method + ". Valid methods are: " + CHI_SQUARE_TEST.label + " and "
                    + FISHER_TEST.label);
        }

        if (method == FISHER_TEST) {
            if (fisherMode == null) {
                throw new AnalysisException("Missing Fisher mode for GWAS. Valid modes are: " + LESS.label + ", " + GREATER.label + ", "
                        + TWO_SIDED.label);
            }
            if (fisherMode != LESS && fisherMode != GREATER && fisherMode != TWO_SIDED) {
                throw new AnalysisException("Invalid Fisher method: " + method + ". Valid methods are: " + LESS.label + ", "
                        + GREATER.label + ", " + TWO_SIDED.label);
            }
        }
    }

    protected void createManhattanPlot() {
    }

    @Override
    protected void exec() throws AnalysisException {
        GwasExecutor gwasExecutor = getAnalysisExecutor(GwasExecutor.class);

        gwasExecutor.setup(executorParams, outDir, configuration);
        gwasExecutor.setStudy(study);
        Path outputFile = outDir.resolve(buildOutputFilename());
        gwasExecutor.setOutputFile(outputFile);

        if (CollectionUtils.isNotEmpty(sampleList1) || CollectionUtils.isNotEmpty(sampleList2)) {
            gwasExecutor.setSampleList1(sampleList1);
            gwasExecutor.setSampleList2(sampleList2);
        } else if (StringUtils.isNotEmpty(phenotype1) || StringUtils.isNotEmpty(phenotype2)) {
            gwasExecutor.setPhenotype1(phenotype1);
            gwasExecutor.setPhenotype2(phenotype2);
        } else if (StringUtils.isNotEmpty(cohort1) || StringUtils.isNotEmpty(cohort2)) {
            gwasExecutor.setCohort1(cohort1);
            gwasExecutor.setCohort2(cohort2);
        } else {
            throw new AnalysisException("Invalid input parameters for GWAS analysis");
        }

        arm.startStep("gwas");
        gwasExecutor.exec();
        arm.endStep(70);

        arm.startStep("manhattan-plot");
        createManhattanPlot();
        arm.endStep(100);

        if (outputFile.toFile().exists()) {
            arm.addFile(outputFile, FileResult.FileType.TAB_SEPARATED);
        }
    }

    protected String buildOutputFilename() throws AnalysisException {
        GwasConfiguration.Method method = configuration.getMethod();
        switch (method) {
            case CHI_SQUARE_TEST:
            case FISHER_TEST:
                return method.label + ".tsv";
            default:
                throw new AnalysisException("Unknown GWAS method: " + method);
        }
    }

    public String getStudy() {
        return study;
    }

    public Gwas setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleList1() {
        return sampleList1;
    }

    public Gwas setSampleList1(List<String> sampleList1) {
        this.sampleList1 = sampleList1;
        return this;
    }

    public List<String> getSampleList2() {
        return sampleList2;
    }

    public Gwas setSampleList2(List<String> sampleList2) {
        this.sampleList2 = sampleList2;
        return this;
    }

    public String getPhenotype1() {
        return phenotype1;
    }

    public Gwas setPhenotype1(String phenotype1) {
        this.phenotype1 = phenotype1;
        return this;
    }

    public String getPhenotype2() {
        return phenotype2;
    }

    public Gwas setPhenotype2(String phenotype2) {
        this.phenotype2 = phenotype2;
        return this;
    }

    public String getCohort1() {
        return cohort1;
    }

    public Gwas setCohort1(String cohort1) {
        this.cohort1 = cohort1;
        return this;
    }

    public String getCohort2() {
        return cohort2;
    }

    public Gwas setCohort2(String cohort2) {
        this.cohort2 = cohort2;
        return this;
    }

    public GwasConfiguration getConfiguration() {
        return configuration;
    }

    public Gwas setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
