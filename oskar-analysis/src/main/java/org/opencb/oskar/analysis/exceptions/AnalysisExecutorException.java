package org.opencb.oskar.analysis.exceptions;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.util.Set;

/**
 * Created by jtarraga on 30/01/17.
 */
public class AnalysisExecutorException extends AnalysisException {

    public AnalysisExecutorException(String msg) {
        super(msg);
    }

    public AnalysisExecutorException(Exception e) {
        super(e);
    }

    public AnalysisExecutorException(String msg, Exception e) {
        super(msg, e);
    }

    public AnalysisExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnalysisExecutorException(Throwable cause) {
        super(cause);
    }

    public static AnalysisExecutorException executorNotFound(Class<?> clazz, String analysis, String executorId,
                                                             AnalysisExecutor.Source source, Set<AnalysisExecutor.Framework> frameworks) {

        String requirements = "";
        if (clazz != OskarAnalysisExecutor.class) {
            requirements = " extending class " + clazz;
        }
        if (StringUtils.isNotEmpty(executorId)) {
            requirements = " with executorId='" + executorId + "'";
        }
        if (source != null) {
            requirements = " for source ='" + source + "'";
        }
        if (CollectionUtils.isNotEmpty(frameworks)) {
            requirements = " for frameworks=" + frameworks;
        }
        requirements += ".";
        return new AnalysisExecutorException("Could not find a valid Analysis executor for the analysis '" + analysis + "'" + requirements);
    }

    public static AnalysisExecutorException cantInstantiate(Class<?> clazz, Exception cause) {
        return new AnalysisExecutorException("Could not create class an instance of class " + clazz, cause);
    }

}
