package org.opencb.oskar.spark.core;

import java.io.IOException;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OskarException extends Exception {

    protected OskarException(String message) {
        super(message);
    }

    protected OskarException(String message, Throwable cause) {
        super(message, cause);
    }

    public static OskarException unsupportedFileFormat(String path) {
        return new OskarException("Unsupported format for file " + path);
    }

    public static OskarException errorLoadingVariantMetadataFile(IOException e, String path) {
        return new OskarException("Error loading variant metadata file " + path, e);
    }

    public static OskarException internalException(RuntimeException e) {
        return internalException("Unexpected internal exception: " + e.getMessage(), e);
    }

    public static OskarException internalException(String msg, RuntimeException e) {
        return new OskarException(msg, e);
    }

}
