package org.opencb.oskar.analysis.variant.gwas;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.util.List;

@Analysis(id = Gwas.ID, data = Analysis.AnalysisData.VARIANT)
public class Gwas extends AbstractAnalysis {

    public static final String ID = "GWAS";

    private List<String> list1;
    private List<String> list2;
    private String phenotype;
    private GwasConfiguration configuration;

    public Gwas(List<String> list1, List<String> list2, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(executorParams, outDir);
        this.list1 = list1;
        this.list2 = list2;
        this.configuration = configuration;
    }

    public Gwas(String phenotype, ObjectMap executorParams, Path outDir, GwasConfiguration configuration) {
        super(executorParams, outDir);
        this.phenotype = phenotype;
        this.configuration = configuration;
    }

    /**
     * Checks if list and list2 are not empty and no common samples exist.
     */
    protected void checkSamples() {

    }

    protected void createManhattanPlot() {

    }

    @Override
    public AnalysisResult execute() throws AnalysisException {
        // checks

        AnalysisResult analysisResult;
        Class executor = getAnalysisExecutorId(executorParams.getString("ID"), Gwas.ID);
        try {
            AbstractGwasExecutor gwasExecutor = (AbstractGwasExecutor) executor.newInstance();
            if (CollectionUtils.isNotEmpty(list1) && CollectionUtils.isNotEmpty(list2)) {
                gwasExecutor.setup(list1, list2, executorParams, outDir, configuration);
            } else if (StringUtils.isNotEmpty(phenotype)) {
                gwasExecutor.setup(phenotype, executorParams, outDir, configuration);
            } else {
                throw new AnalysisException("Invalid input parameters for GWAS analysis");
            }
            analysisResult = gwasExecutor.exec();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AnalysisException("Error when executing GWAS analysis", e);
        }

        return analysisResult;
    }

}
