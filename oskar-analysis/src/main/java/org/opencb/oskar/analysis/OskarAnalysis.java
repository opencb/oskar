package org.opencb.oskar.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.OskarAnalysisException;

import java.nio.file.Path;

public abstract class OskarAnalysis {

    protected ObjectMap params;
    protected Path outDir;

    protected OskarAnalysis() {
    }

    protected OskarAnalysis(ObjectMap params, Path outDir) {
        setUp(params, outDir);
    }

    public final void setUp(ObjectMap executorParams, Path outDir) {
        this.params = executorParams;
        this.outDir = outDir;
    }

    /**
     * Method to be implemented by subclasses with the actual execution.
     * @throws OskarAnalysisException on error
     */
    public abstract void exec() throws OskarAnalysisException;

    public final ObjectMap getParams() {
        return params;
    }

    public final Path getOutDir() {
        return outDir;
    }
}
