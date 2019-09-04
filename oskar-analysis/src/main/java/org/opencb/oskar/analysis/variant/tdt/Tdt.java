package org.opencb.oskar.analysis.variant.tdt;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysis;
import org.opencb.oskar.analysis.AnalysisResult;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = Tdt.ID, data = Analysis.AnalysisData.VARIANT)
public class Tdt extends AbstractAnalysis {

    public static final String ID = "TDT";

    private String phenotype;

    public Tdt(String phenotype, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.phenotype = phenotype;
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
        Class executor = getAnalysisExecutorId(executorParams.getString("ID"), Tdt.ID);
        try {
            AbstractTdtExecutor tdtExecutor = (AbstractTdtExecutor) executor.newInstance();
            tdtExecutor.setup(phenotype, executorParams, outDir);
            analysisResult = tdtExecutor.exec();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AnalysisException("Error when executing TDT analysis", e);
        }

        return analysisResult;
    }
}
