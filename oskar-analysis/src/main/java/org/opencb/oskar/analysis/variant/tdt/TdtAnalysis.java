package org.opencb.oskar.analysis.variant.tdt;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;

import java.nio.file.Path;

public abstract class TdtAnalysis extends OskarAnalysis {

    private String phenotype;
    private String study;

    public TdtAnalysis() {
    }

    public TdtAnalysis(String phenotype, ObjectMap executorParams, Path outDir) {
        this.setup(phenotype, executorParams, outDir);
    }

    protected void setup(String phenotype, ObjectMap executorParams, Path outDir) {
        super.setUp(executorParams, outDir);
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

    public String getPhenotype() {
        return phenotype;
    }

    public TdtAnalysis setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public TdtAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }
}
