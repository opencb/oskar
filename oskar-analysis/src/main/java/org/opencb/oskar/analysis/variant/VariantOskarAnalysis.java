package org.opencb.oskar.analysis.variant;

import org.opencb.oskar.analysis.OskarAnalysis;
import org.opencb.oskar.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/05/17.
 */
public abstract class VariantOskarAnalysis extends OskarAnalysis {

    protected VariantOskarAnalysis(String studyId, OskarConfiguration configuration) {
        super(studyId, configuration);
    }
}
