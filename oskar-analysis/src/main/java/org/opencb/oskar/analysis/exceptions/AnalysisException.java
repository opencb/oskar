package org.opencb.oskar.analysis.exceptions;

/**
 * Created by pfurio on 23/05/17.
 */
public class AnalysisException extends Exception {

    public AnalysisException(String message) {
        super(message);
    }

    public AnalysisException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnalysisException(Throwable cause) {
        super(cause);
    }

}
