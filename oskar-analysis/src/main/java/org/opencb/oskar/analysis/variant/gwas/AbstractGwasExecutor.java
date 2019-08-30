package org.opencb.oskar.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractGwasExecutor extends AbstractAnalysisExecutor {

    private List<String> list1;
    private List<String> list2;
    private String phenotype;
    private ObjectMap executorParams;
    private Path outDir;
    private GwasConfiguration configuration;

    public AbstractGwasExecutor() {
    }

    public AbstractGwasExecutor(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir,
                                GwasConfiguration configuration) {
        this.setup(list1, list2, executorParams,  outDir, configuration);
    }

    public AbstractGwasExecutor(String phenotype, ObjectMap executorParams, Path outDir,
                                GwasConfiguration configuration) {
        this.setup(phenotype, executorParams,  outDir, configuration);
    }

    protected void setup(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        this.list1 = list1;
        this.list2 = list2;
        this.phenotype = null;
        this.executorParams = executorParams;
        this.outDir = outDir;
        this.configuration = configuration;
    }

    protected void setup(String phenotype, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        this.list1 = null;
        this.list2 = null;
        this.phenotype = phenotype;
        this.executorParams = executorParams;
        this.outDir = outDir;
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

    protected AnalysisResult createAnalysisResult() throws AnalysisException {
        List<AnalysisResult.File> resultFiles = new ArrayList<>();
        String outFilename = getOutDir() + "/" + getOutputFilename();
        if (new File(outFilename).exists()) {
            resultFiles.add(new AnalysisResult.File(Paths.get(outFilename), AnalysisResult.FileType.TAB_SEPARATED));
        }

        return new AnalysisResult().setAnalysisId(Gwas.ID).setDateTime(getDateTime()).setExecutorParams(executorParams)
                .setOutputFiles(resultFiles);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractGwasExecutor{");
        sb.append("list1=").append(list1);
        sb.append(", list2=").append(list2);
        sb.append(", phenotype='").append(phenotype).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outDir=").append(outDir);
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getList1() {
        return list1;
    }

    public AbstractGwasExecutor setList1(List<String> list1) {
        this.list1 = list1;
        return this;
    }

    public List<String> getList2() {
        return list2;
    }

    public AbstractGwasExecutor setList2(List<String> list2) {
        this.list2 = list2;
        return this;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public AbstractGwasExecutor setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    public ObjectMap getExecutorParams() {
        return executorParams;
    }

    public AbstractGwasExecutor setExecutorParams(ObjectMap executorParams) {
        this.executorParams = executorParams;
        return this;
    }

    public Path getOutDir() {
        return outDir;
    }

    public AbstractGwasExecutor setOutDir(Path outDir) {
        this.outDir = outDir;
        return this;
    }

    public GwasConfiguration getConfiguration() {
        return configuration;
    }

    public AbstractGwasExecutor setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
