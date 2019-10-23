package org.opencb.oskar.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class GwasAnalysis extends OskarAnalysis {

    public static final String ID = "GWAS";
    private String study;
    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype1;
    private String phenotype2;
    private String cohort1;
    private String cohort2;
    private Path outputFile;
    private GwasConfiguration configuration;

    public GwasAnalysis() {
    }

    public GwasAnalysis(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        this.setup(executorParams, outDir, configuration);
    }

    protected void setup(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super.setUp(executorParams, outDir);
        this.configuration = configuration;
    }

    protected List<String> getHeaderColumns() {
        List<String> columns = new ArrayList<>();
        columns.add("chromosome");
        columns.add("start");
        columns.add("end");
        columns.add("reference");
        columns.add("alternate");
        columns.add("dbSNP");
        columns.add("gene");
        columns.add("biotype");
        columns.add("consequence-types");
        if (configuration.getMethod() == GwasConfiguration.Method.CHI_SQUARE_TEST) {
            columns.add("chi-square");
        }
        columns.add("p-value");
        columns.add("odd-ratio");
        return columns;
    }

    protected String getHeaderLine() {
        return "#" + String.join("\t", getHeaderColumns());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GwasAnalysis{");
        sb.append("sampleList1=").append(sampleList1);
        sb.append(", sampleList2=").append(sampleList2);
        sb.append(", phenotype1='").append(phenotype1).append('\'');
        sb.append(", phenotype2='").append(phenotype2).append('\'');
        sb.append(", cohort1='").append(cohort1).append('\'');
        sb.append(", cohort2='").append(cohort2).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append(", params=").append(params);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public GwasAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSampleList1() {
        return sampleList1;
    }

    public GwasAnalysis setSampleList1(List<String> sampleList1) {
        this.sampleList1 = sampleList1;
        return this;
    }

    public List<String> getSampleList2() {
        return sampleList2;
    }

    public GwasAnalysis setSampleList2(List<String> sampleList2) {
        this.sampleList2 = sampleList2;
        return this;
    }

    public String getPhenotype1() {
        return phenotype1;
    }

    public GwasAnalysis setPhenotype1(String phenotype1) {
        this.phenotype1 = phenotype1;
        return this;
    }

    public String getPhenotype2() {
        return phenotype2;
    }

    public GwasAnalysis setPhenotype2(String phenotype2) {
        this.phenotype2 = phenotype2;
        return this;
    }

    public String getCohort1() {
        return cohort1;
    }

    public GwasAnalysis setCohort1(String cohort1) {
        this.cohort1 = cohort1;
        return this;
    }

    public String getCohort2() {
        return cohort2;
    }

    public GwasAnalysis setCohort2(String cohort2) {
        this.cohort2 = cohort2;
        return this;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public GwasAnalysis setOutputFile(Path outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public GwasConfiguration getConfiguration() {
        return configuration == null ? new GwasConfiguration() : configuration;
    }

    public GwasAnalysis setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
