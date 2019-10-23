package org.opencb.oskar.analysis.exceptions;

import org.opencb.oskar.core.exceptions.OskarException;

/**
 * Created by pfurio on 23/05/17.
 */
public class OskarAnalysisException extends OskarException {

    public OskarAnalysisException(String message) {
        super(message);
    }

    public OskarAnalysisException(String message, Throwable cause) {
        super(message, cause);
    }

    public OskarAnalysisException(Throwable cause) {
        super(cause);
    }

}
