package org.opencb.oskar.spark.variant.analysis.wrappers;

import org.opencb.oskar.analysis.variant.VariantAnalysisExecutor;
import org.opencb.oskar.core.config.OskarConfiguration;

public abstract class VariantAnalysisWrapper extends VariantAnalysisExecutor {

    protected VariantAnalysisWrapper(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
