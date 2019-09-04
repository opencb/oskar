package org.opencb.oskar.analysis.variant.tdt;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTdtExecutor extends AbstractAnalysisExecutor {

    private String phenotype;

    public AbstractTdtExecutor() {
    }

    public AbstractTdtExecutor(String phenotype, ObjectMap executorParams, Path outDir) {
        this.setup(phenotype, executorParams, outDir);
    }

    protected void setup(String phenotype, ObjectMap executorParams, Path outDir) {
        super.setup(executorParams, outDir);
        this.phenotype = phenotype;
    }

    protected String getHeaderLine() {
        StringBuilder sb = new StringBuilder("#");
        sb.append("chromosome").append("\t").append("start").append("\t").append("end").append("\t").append("strand").append("\t")
                .append("reference").append("\t").append("alternate").append("\t").append("dbSNP").append("\t").append("gene").append("\t")
                .append("biotype").append("\t").append("conseq. types").append("\t")
                .append("chi square").append("\t").append("p-value").append("\t").append("odd ratio").append("\t")
                .append("freedom degrees").append("\t").append("t1").append("\t").append("t2");
        return sb.toString();
    }

    protected AnalysisResult createAnalysisResult() throws AnalysisException {
        List<AnalysisResult.File> resultFiles = new ArrayList<>();
        String outFilename = getOutDir() + "/tdt.txt";
        if (new File(outFilename).exists()) {
            resultFiles.add(new AnalysisResult.File(Paths.get(outFilename), AnalysisResult.FileType.TAB_SEPARATED));
        }

        return new AnalysisResult().setAnalysisId(Tdt.ID).setDateTime(getDateTime()).setExecutorParams(executorParams)
                .setOutputFiles(resultFiles);
    }

    public String getPhenotype() {
        return phenotype;
    }

    public AbstractTdtExecutor setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }
}
