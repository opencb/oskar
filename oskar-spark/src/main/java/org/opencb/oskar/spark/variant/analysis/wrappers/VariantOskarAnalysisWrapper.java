package org.opencb.oskar.spark.variant.analysis.wrappers;

import org.opencb.oskar.analysis.variant.VariantOskarAnalysis;
import org.opencb.oskar.core.config.OskarConfiguration;

public abstract class VariantOskarAnalysisWrapper extends VariantOskarAnalysis {

    protected VariantOskarAnalysisWrapper(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
