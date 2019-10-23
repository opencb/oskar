package org.opencb.oskar.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.ExecutionException;

import java.nio.file.Path;

public abstract class OskarExecutor {

    protected ObjectMap params;
    protected Path outDir;

    protected OskarExecutor() {
    }

    protected OskarExecutor(ObjectMap params, Path outDir) {
        setUp(params, outDir);
    }

    public final void setUp(ObjectMap executorParams, Path outDir) {
        this.params = executorParams;
        this.outDir = outDir;
    }

    /**
     * Method to be implemented by subclasses with the actual execution.
     * @throws ExecutionException on error
     */
    public abstract void exec() throws ExecutionException;

    public final ObjectMap getParams() {
        return params;
    }

    public final Path getOutDir() {
        return outDir;
    }
}
