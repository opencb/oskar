package org.opencb.oskar.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class AbstractAnalysisExecutor {

    protected ObjectMap executorParams;
    protected Path outDir;

    protected AbstractAnalysisExecutor() {
    }

    protected AbstractAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        setup(executorParams, outDir);
    }

    protected void setup(ObjectMap executorParams, Path outDir) {
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public abstract AnalysisResult exec() throws AnalysisException;

    public String getDateTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public ObjectMap getExecutorParams() {
        return executorParams;
    }

    public AbstractAnalysisExecutor setExecutorParams(ObjectMap executorParams) {
        this.executorParams = executorParams;
        return this;
    }

    public Path getOutDir() {
        return outDir;
    }

    public AbstractAnalysisExecutor setOutDir(Path outDir) {
        this.outDir = outDir;
        return this;
    }
}
