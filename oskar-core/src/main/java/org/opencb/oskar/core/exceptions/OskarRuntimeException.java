package org.opencb.oskar.core.exceptions;

/**
 * Created on 23/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OskarRuntimeException extends RuntimeException {

    // Do not use constructor from outside. Add a factory method in OskarException.
    protected OskarRuntimeException(String message) {
        super(message);
    }

    // Do not use constructor from outside. Add a factory method in OskarException.
    protected OskarRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
