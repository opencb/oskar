package org.opencb.oskar.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public abstract class GwasExecutor extends OskarAnalysisExecutor {

    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype1;
    private String phenotype2;
    private String cohort1;
    private String cohort2;
    private GwasConfiguration configuration;

    public GwasExecutor() {
    }

    public GwasExecutor(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        this.setup(executorParams, outDir, configuration);
    }

    protected void setup(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super.setup(executorParams, outDir);
        this.configuration = configuration;
    }

    protected String getHeaderLine() {
        StringBuilder sb = new StringBuilder("#");
        sb.append("chromosome").append("\t").append("start").append("\t").append("end").append("\t").append("strand").append("\t")
                .append("reference").append("\t").append("alternate").append("\t").append("dbSNP").append("\t").append("gene").append("\t")
                .append("biotype").append("\t").append("consequence-types");
        if (configuration.getMethod() == GwasConfiguration.Method.CHI_SQUARE_TEST) {
            sb.append("\t").append("chi-square");
        }
        sb.append("\t").append("p-value").append("\t").append("odd-ratio");
        return sb.toString();
    }

    protected String getOutputFilename() throws AnalysisException {
        GwasConfiguration.Method method = configuration.getMethod();
        switch (method) {
            case CHI_SQUARE_TEST:
            case FISHER_TEST:
                return method.label + ".txt";
            default:
                throw new AnalysisException("Unknown GWAS method: " + method);
        }
    }

    protected void registerFiles() throws AnalysisException {
        String outFilename = getOutDir() + "/" + getOutputFilename();
        if (new File(outFilename).exists()) {
            arm.addFile(Paths.get(outFilename), FileResult.FileType.TAB_SEPARATED);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GwasExecutor{");
        sb.append("sampleList1=").append(sampleList1);
        sb.append(", sampleList2=").append(sampleList2);
        sb.append(", phenotype1='").append(phenotype1).append('\'');
        sb.append(", phenotype2='").append(phenotype2).append('\'');
        sb.append(", cohort1='").append(cohort1).append('\'');
        sb.append(", cohort2='").append(cohort2).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append(", arm=").append(arm);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getSampleList1() {
        return sampleList1;
    }

    public GwasExecutor setSampleList1(List<String> sampleList1) {
        this.sampleList1 = sampleList1;
        return this;
    }

    public List<String> getSampleList2() {
        return sampleList2;
    }

    public GwasExecutor setSampleList2(List<String> sampleList2) {
        this.sampleList2 = sampleList2;
        return this;
    }

    public String getPhenotype1() {
        return phenotype1;
    }

    public GwasExecutor setPhenotype1(String phenotype1) {
        this.phenotype1 = phenotype1;
        return this;
    }

    public String getPhenotype2() {
        return phenotype2;
    }

    public GwasExecutor setPhenotype2(String phenotype2) {
        this.phenotype2 = phenotype2;
        return this;
    }

    public String getCohort1() {
        return cohort1;
    }

    public GwasExecutor setCohort1(String cohort1) {
        this.cohort1 = cohort1;
        return this;
    }

    public String getCohort2() {
        return cohort2;
    }

    public GwasExecutor setCohort2(String cohort2) {
        this.cohort2 = cohort2;
        return this;
    }


    public GwasConfiguration getConfiguration() {
        return configuration;
    }

    public GwasExecutor setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
