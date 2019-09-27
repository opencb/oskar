package org.opencb.oskar.analysis.variant.tdt;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.FileResult;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class TdtExecutor extends OskarAnalysisExecutor {

    private String phenotype;

    public TdtExecutor() {
    }

    public TdtExecutor(String phenotype, ObjectMap executorParams, Path outDir) {
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

    protected void registerFiles() throws AnalysisException {
        String outFilename = getOutDir() + "/tdt.txt";
        if (new File(outFilename).exists()) {
            arm.addFile(Paths.get(outFilename), FileResult.FileType.TAB_SEPARATED);
        }
    }

    public String getPhenotype() {
        return phenotype;
    }

    public TdtExecutor setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }
}
