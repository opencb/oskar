package org.opencb.oskar.analysis.exceptions;

/**
 * Created by pfurio on 23/05/17.
 */
public class ExecutionException extends Exception {

    public ExecutionException(String message) {
        super(message);
    }

    public ExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecutionException(Throwable cause) {
        super(cause);
    }

}
