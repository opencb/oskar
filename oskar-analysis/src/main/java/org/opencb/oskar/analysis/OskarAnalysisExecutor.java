package org.opencb.oskar.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.result.AnalysisResultManager;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.nio.file.Path;

public abstract class OskarAnalysisExecutor {

    protected ObjectMap executorParams;
    protected Path outDir;
    protected AnalysisResultManager arm;

    protected OskarAnalysisExecutor() {
    }

    protected OskarAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        setup(executorParams, outDir);
    }

    public final void init(AnalysisResultManager arm) {
        this.arm = arm;
    }

    public final String getAnalysisId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).analysis();
    }

    public final String getId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).id();
    }

    public void setup(ObjectMap executorParams, Path outDir) {
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public abstract void exec() throws AnalysisException;

    public final ObjectMap getExecutorParams() {
        return executorParams;
    }

    public final Path getOutDir() {
        return outDir;
    }
}
