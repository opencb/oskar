package org.opencb.oskar.analysis.variant.gwas;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = Gwas.ID, data = Analysis.AnalysisData.VARIANT)
public class Gwas extends OskarAnalysis {

    public static final String ID = "GWAS";

    private List<String> sampleList1;
    private List<String> sampleList2;
    private String phenotype1;
    private String phenotype2;
    private String cohort1;
    private String cohort2;
    private GwasConfiguration configuration;

    public Gwas(ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(executorParams, outDir);
        this.configuration = configuration;
    }

    /**
     * Checks if sampleList1 and sampleList2 are not empty and no common samples exist.
     */
    @Override
    protected void check() {
        // checks
    }

    protected void createManhattanPlot() {
    }

    @Override
    protected void exec() throws AnalysisException {
        GwasExecutor gwasExecutor = getAnalysisExecutor(GwasExecutor.class, executorParams.getString("ID"));

        gwasExecutor.setup(executorParams, outDir, configuration);
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
