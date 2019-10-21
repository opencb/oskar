package org.opencb.oskar.analysis.variant.tdt;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;

@Analysis(id = Tdt.ID, data = Analysis.AnalysisData.VARIANT)
public class Tdt extends OskarAnalysis {

    public static final String ID = "TDT";

    private String phenotype;

    public Tdt(String phenotype, ObjectMap executorParams, Path outDir) {
        super(executorParams, outDir);
        this.phenotype = phenotype;
    }

    protected void createManhattanPlot() {
    }

    /**
     * Checks if list and list2 are not empty and no common samples exist.
     */
    @Override
    protected void check() throws AnalysisException {
        // checks
    }

    @Override
    public void exec() throws AnalysisException {
        TdtExecutor tdtExecutor = getAnalysisExecutor(TdtExecutor.class);
        tdtExecutor.setup(phenotype, executorParams, outDir);

        arm.startStep("tdt");
        tdtExecutor.exec();
        arm.endStep(100);
    }
}
