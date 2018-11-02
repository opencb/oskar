package org.opencb.oskar.analysis.variant;

import org.opencb.oskar.analysis.AnalysisExecutor;
import org.opencb.oskar.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantAnalysisExecutor extends AnalysisExecutor {

    protected VariantAnalysisExecutor(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
