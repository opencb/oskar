package org.opencb.oskar.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.core.annotations.Analysis;

import java.util.List;

@Analysis(id = Gwas.ID, data = Analysis.AnalysisData.VARIANT)
public class Gwas extends AbstractAnalysis {

    private List<String> list1;
    private List<String> list2;
    private ObjectMap executorParams;
    private GwasConfiguration configuration;

    public static final String ID = "GWAS";

    public Gwas(List<String> list1, List<String> list2, ObjectMap executorParams, GwasConfiguration configuration) {
        this.list1 = list1;
        this.list2 = list2;
        this.executorParams = executorParams;
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
    public void execute() {
        // checks

        Class executor = getAnalysisExecutorId(executorParams.getString("ID"), Gwas.ID);
        try {
            AbstractGwasExecutor gwasExecutor = (AbstractGwasExecutor) executor.newInstance();
            gwasExecutor.setup(list1, list2, executorParams, configuration);
            gwasExecutor.exec();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
