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

    private List<String> list1;
    private List<String> list2;
    private String phenotype;
    private GwasConfiguration configuration;

    public GwasExecutor() {
    }

    public GwasExecutor(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir,
                        GwasConfiguration configuration) {
        this.setup(list1, list2, executorParams, outDir, configuration);
    }

    public GwasExecutor(String phenotype, ObjectMap executorParams, Path outDir,
                        GwasConfiguration configuration) {
        this.setup(phenotype, executorParams, outDir, configuration);
    }

    protected void setup(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super.setup(executorParams, outDir);
        this.list1 = list1;
        this.list2 = list2;
        this.phenotype = null;
        this.configuration = configuration;
    }

    protected void setup(String phenotype, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super.setup(executorParams, outDir);
        this.list1 = null;
        this.list2 = null;
        this.phenotype = phenotype;
        this.configuration = configuration;
    }

    protected String getHeaderLine() {
        StringBuilder sb = new StringBuilder("#");
        sb.append("chromosome").append("\t").append("start").append("\t").append("end").append("\t").append("strand").append("\t")
                .append("reference").append("\t").append("alternate").append("\t").append("dbSNP").append("\t").append("gene").append("\t")
                .append("biotype").append("\t").append("conseq. types");
        if (configuration.getMethod() == GwasConfiguration.Method.CHI_SQUARE_TEST) {
            sb.append("\t").append("chi square");
        }
        sb.append("\t").append("p-value").append("\t").append("odd ratio");
        return sb.toString();
    }

    protected String getOutputFilename() throws AnalysisException {
        switch (configuration.getMethod()) {
            case CHI_SQUARE_TEST:
                return "gwas_chisquare.txt";
            case FISHER_TEST:
                return "gwas_fisher.txt";
            default:
                throw new AnalysisException("Unknown GWAS method: " + configuration.getMethod());
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
        sb.append("list1=").append(list1);
        sb.append(", list2=").append(list2);
        sb.append(", phenotype='").append(phenotype).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getList1() {
        return list1;
    }

    public GwasExecutor setList1(List<String> list1) {
        this.list1 = list1;
        return this;
    }

    public List<String> getList2() {
        return list2;
    }

    public GwasExecutor setList2(List<String> list2) {
        this.list2 = list2;
        return this;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public GwasExecutor setPhenotype(String phenotype) {
        this.phenotype = phenotype;
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
