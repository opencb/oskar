package org.opencb.oskar.spark.commons;

import java.io.IOException;
import java.util.Collection;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OskarException extends Exception {

    // Do not use constructor from outside. Add a factory method.
    protected OskarException(String message) {
        super(message);
    }

    // Do not use constructor from outside. Add a factory method.
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

    public static OskarException unknownStudy(String study, Collection<String> studies) {
        return new OskarException("Unknown study '" + study + "'. Select one from " + studies);
    }
}
