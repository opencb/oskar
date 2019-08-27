package org.opencb.oskar.analysis;

import org.apache.commons.lang3.StringUtils;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.util.Set;

public abstract class AbstractAnalysis {


    protected Class getAnalysisExecutorId(String analysisExecutorId) {
        return getAnalysisExecutorId(analysisExecutorId, "");
    }

    protected Class getAnalysisExecutorId(String analysisExecutorId, String analysisId) {
        Reflections reflections = new Reflections("", new SubTypesScanner(), new TypeAnnotationsScanner());
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(AnalysisExecutor.class);
        for (Class<?> aClass : typesAnnotatedWith) {
            AnalysisExecutor annotation = aClass.getAnnotation(AnalysisExecutor.class);
            if (StringUtils.isNotEmpty(analysisId)) {
                if (annotation.id().equals(analysisExecutorId) && annotation.analysis().equals(analysisId)) {
                    return aClass;
                }
            } else {
                if (annotation.id().equals(analysisExecutorId)) {
                    return aClass;
                }
            }
        }
        return null;
    }

    public abstract void execute();

}
