package org.opencb.oskar.analysis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.analysis.result.AnalysisResult;
import org.opencb.oskar.analysis.result.AnalysisResultManager;
import org.opencb.oskar.core.annotations.Analysis;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.nio.file.Path;
import java.util.*;

public abstract class OskarAnalysis {

    protected ObjectMap executorParams;
    protected Path outDir;
    protected AnalysisExecutor.Source source;
    protected Set<AnalysisExecutor.Framework> availableFrameworks;

    protected AnalysisResultManager arm;

    public OskarAnalysis() {
    }

    public OskarAnalysis(ObjectMap executorParams, Path outDir) {
        setUp(executorParams, outDir, null, null);
    }

    /**
     * Setup the analysis providing the parameters required for the execution.
     * @param executorParams        Params to be provided to the Executor
     * @param outDir                Output directory
     * @param inputDataSource       Input data source type
     * @param availableFrameworks   Available frameworks in this environment
     */
    public final void setUp(ObjectMap executorParams, Path outDir,
                            AnalysisExecutor.Source inputDataSource,
                            Collection<AnalysisExecutor.Framework> availableFrameworks) {
        this.executorParams = executorParams;
        this.outDir = outDir;
        this.source = inputDataSource;
        this.availableFrameworks = availableFrameworks == null ? null : new HashSet<>(availableFrameworks);
    }

    /**
     * Execute the analysis. The analysis should have been properly setup before being executed.
     *
     * @return AnalysisResult
     * @throws AnalysisException on error
     */
    public final AnalysisResult execute() throws AnalysisException {
        arm = new AnalysisResultManager(outDir);
        arm.init(getId(), executorParams);
        try {
            check();
            exec();
            return arm.close();
        } catch (Exception e) {
            arm.close(e);
            throw e;
        }
    }

    /**
     * Check that the given parameters are correct.
     * This method will be called before the {@link #exec()}.
     *
     * @throws AnalysisException if the parameters are not correct
     */
    protected void check() throws AnalysisException {
    }

    /**
     * Method to be implemented by subclasses with the actual execution of the analysis.
     * @throws AnalysisException on error
     */
    protected abstract void exec() throws AnalysisException;

    /**
     * @return the analysis id
     */
    public final String getId() {
        return this.getClass().getAnnotation(Analysis.class).id();
    }

    protected final Class<? extends OskarAnalysisExecutor> getAnalysisExecutorClass(String analysisExecutorId) {
        return getAnalysisExecutorClass(OskarAnalysisExecutor.class, analysisExecutorId, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> Class<? extends T> getAnalysisExecutorClass(
            Class<T> clazz, String analysisExecutorId, AnalysisExecutor.Source source,
            Set<AnalysisExecutor.Framework> availableFrameworks) {
        Objects.requireNonNull(clazz);
        String analysisId = getId();

        if (source == null) {
            source = this.source;
        }
        if (CollectionUtils.isEmpty(availableFrameworks)) {
            availableFrameworks = this.availableFrameworks;
        }

        TypeAnnotationsScanner annotationsScanner = new TypeAnnotationsScanner();
        annotationsScanner.setResultFilter(s -> StringUtils.equals(s, AnalysisExecutor.class.getName()));

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(), annotationsScanner)
                .addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class")));

        Set<Class<? extends T>> typesAnnotatedWith = reflections.getSubTypesOf(clazz);
        for (Class<? extends T> aClass : typesAnnotatedWith) {
            AnalysisExecutor annotation = aClass.getAnnotation(AnalysisExecutor.class);
            if (annotation != null) {
                if (annotation.analysis().equals(analysisId)) {
                    if (StringUtils.isEmpty(analysisExecutorId) || analysisExecutorId.equals(annotation.id())) {
                        if (source == null || annotation.source() == source) {
                            if (CollectionUtils.isEmpty(availableFrameworks) || availableFrameworks.contains(annotation.framework())) {
                                return aClass;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    protected final OskarAnalysisExecutor getAnalysisExecutor()
            throws AnalysisExecutorException {
        return getAnalysisExecutor(OskarAnalysisExecutor.class, null, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, null, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz, String analysisExecutorId)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, analysisExecutorId, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(
            Class<T> clazz, String analysisExecutorId, AnalysisExecutor.Source source,
            Set<AnalysisExecutor.Framework> availableFrameworks)
            throws AnalysisExecutorException {
        Class<? extends T> executorClass = getAnalysisExecutorClass(clazz, analysisExecutorId, source, availableFrameworks);
        if (executorClass == null) {
            throw AnalysisExecutorException.executorNotFound(clazz, getId(), analysisExecutorId, source, availableFrameworks);
        }
        try {
            T t = executorClass.newInstance();
            t.init(arm);

            // Update executor ID, if necessary
            if (StringUtils.isEmpty(analysisExecutorId)) {
                arm.updateResult(analysisResult -> {
                    String executorId = t.getClass().getAnnotation(AnalysisExecutor.class).id();
                    analysisResult.setExecutorId(executorId);
                    analysisResult.getExecutorParams().put("ID", executorId);
                });
            }

            return t;
        } catch (InstantiationException | IllegalAccessException | AnalysisException e) {
            throw AnalysisExecutorException.cantInstantiate(executorClass, e);
        }
    }

}
