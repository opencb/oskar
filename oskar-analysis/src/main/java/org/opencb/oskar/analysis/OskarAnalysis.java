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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

public abstract class OskarAnalysis {

    public static final String EXECUTOR_ID = "ID";
    protected ObjectMap executorParams;
    protected Path outDir;
    protected List<AnalysisExecutor.Source> sourceTypes;
    protected List<AnalysisExecutor.Framework> availableFrameworks;
    private Logger logger = LoggerFactory.getLogger(OskarAnalysis.class);

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
     * @param inputDataSourceTypes  Input data source types
     * @param availableFrameworks   Available frameworks in this environment
     */
    public final void setUp(ObjectMap executorParams, Path outDir,
                            List<AnalysisExecutor.Source> inputDataSourceTypes,
                            List<AnalysisExecutor.Framework> availableFrameworks) {
        this.executorParams = executorParams;
        this.outDir = outDir;
        this.sourceTypes = inputDataSourceTypes;
        this.availableFrameworks = availableFrameworks == null ? null : new ArrayList<>(availableFrameworks);
    }

    /**
     * Execute the analysis. The analysis should have been properly setUp before being executed.
     *
     * @return AnalysisResult
     * @throws AnalysisException on error
     */
    public final AnalysisResult execute() throws AnalysisException {
        arm = new AnalysisResultManager(outDir);
        arm.init(getId(), executorParams);
        try {
            execute(arm);
            return arm.close();
        } catch (Exception e) {
            arm.close(e);
            throw e;
        }
    }

    public final void execute(AnalysisResultManager arm) throws AnalysisException {
        this.arm = arm;
        check();
        exec();
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

    /**
     * @return the analysis id
     */
    public final Analysis.AnalysisData getAnalysisData() {
        return this.getClass().getAnnotation(Analysis.class).data();
    }

    public final OskarAnalysis addSource(AnalysisExecutor.Source source) {
        if (sourceTypes == null) {
            sourceTypes = new ArrayList<>();
        }
        sourceTypes.add(source);
        return this;
    }

    public final OskarAnalysis addFramework(AnalysisExecutor.Framework framework) {
        if (availableFrameworks == null) {
            availableFrameworks = new ArrayList<>();
        }
        availableFrameworks.add(framework);
        return this;
    }

    protected final Class<? extends OskarAnalysisExecutor> getAnalysisExecutorClass(String analysisExecutorId) {
        return getAnalysisExecutorClass(OskarAnalysisExecutor.class, analysisExecutorId, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> Class<? extends T> getAnalysisExecutorClass(
            Class<T> clazz, String analysisExecutorId, List<AnalysisExecutor.Source> sourceTypes,
            List<AnalysisExecutor.Framework> availableFrameworks) {
        Objects.requireNonNull(clazz);
        String analysisId = getId();

        if (sourceTypes == null) {
            sourceTypes = this.sourceTypes;
        }
        if (CollectionUtils.isEmpty(availableFrameworks)) {
            availableFrameworks = this.availableFrameworks;
        }

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, AnalysisExecutor.class.getName())))
                .addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class"))
        );

        Set<Class<? extends T>> typesAnnotatedWith = reflections.getSubTypesOf(clazz);
        List<Class<? extends T>> matchedClasses = new ArrayList<>();
        for (Class<? extends T> aClass : typesAnnotatedWith) {
            AnalysisExecutor annotation = aClass.getAnnotation(AnalysisExecutor.class);
            if (annotation != null) {
                if (annotation.analysis().equals(analysisId)) {
                    if (StringUtils.isEmpty(analysisExecutorId) || analysisExecutorId.equals(annotation.id())) {
                        if (sourceTypes == null || sourceTypes.contains(annotation.source())) {
                            if (CollectionUtils.isEmpty(availableFrameworks) || availableFrameworks.contains(annotation.framework())) {
                                matchedClasses.add(aClass);
                            }
                        }
                    }
                }
            }
        }
        if (matchedClasses.isEmpty()) {
            return null;
        } else if (matchedClasses.size() == 1) {
            return matchedClasses.get(0);
        } else {
            logger.info("Found multiple AnalysisExecutor candidates.");
            for (Class<? extends T> matchedClass : matchedClasses) {
                logger.info(" - " + matchedClass);
            }
            logger.info("Sort by framework and source preference.");

            // Prefer the executor that matches better with the source
            // Prefer the executor that matches better with the framework
            List<AnalysisExecutor.Framework> finalAvailableFrameworks =
                    availableFrameworks == null ? Collections.emptyList() : availableFrameworks;
            List<AnalysisExecutor.Source> finalSourceTypes =
                    sourceTypes == null ? Collections.emptyList() : sourceTypes;

            Comparator<Class<? extends T>> comparator = Comparator.<Class<? extends T>>comparingInt(c1 -> {
                AnalysisExecutor annot1 = c1.getAnnotation(AnalysisExecutor.class);
                return finalAvailableFrameworks.indexOf(annot1.framework());
            }).thenComparingInt(c -> {
                AnalysisExecutor annot = c.getAnnotation(AnalysisExecutor.class);
                return finalSourceTypes.indexOf(annot.source());
            }).thenComparing(Class::getName);

            matchedClasses.sort(comparator);

            return matchedClasses.get(0);
        }
    }

    protected final OskarAnalysisExecutor getAnalysisExecutor()
            throws AnalysisExecutorException {
        return getAnalysisExecutor(OskarAnalysisExecutor.class, null, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, executorParams.getString(OskarAnalysis.EXECUTOR_ID), null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz, String analysisExecutorId)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, analysisExecutorId, null, null);
    }

    protected final <T extends OskarAnalysisExecutor> T getAnalysisExecutor(
            Class<T> clazz, String analysisExecutorId, List<AnalysisExecutor.Source> source,
            List<AnalysisExecutor.Framework> availableFrameworks)
            throws AnalysisExecutorException {
        Class<? extends T> executorClass = getAnalysisExecutorClass(clazz, analysisExecutorId, source, availableFrameworks);
        if (executorClass == null) {
            throw AnalysisExecutorException.executorNotFound(clazz, getId(), analysisExecutorId, source, availableFrameworks);
        }
        try {
            T t = executorClass.newInstance();
            logger.info("Using AnalysisExecutor '" + t.getId() + "' : " + executorClass);
            t.init(arm);

            // Update executor ID
            if (arm != null) {
                arm.updateResult(analysisResult -> {
                    String executorId = t.getId();
                    analysisResult.setExecutorId(executorId);
                    analysisResult.getExecutorParams().put(EXECUTOR_ID, executorId);
                });

            }
            t.setUp(executorParams, outDir);

            return t;
        } catch (InstantiationException | IllegalAccessException | AnalysisException e) {
            throw AnalysisExecutorException.cantInstantiate(executorClass, e);
        }
    }

}
