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

    public static OskarRuntimeException unsupportedFileFormat(String path) {
        return new OskarRuntimeException("Unsupported format for file " + path);
    }

    public static OskarException errorLoadingVariantMetadataFile(IOException e, String path) {
        return new OskarException("Error loading variant metadata file " + path, e);
    }

    public static OskarRuntimeException errorReadingVariantMetadataFromDataframe(IOException e) {
        return new OskarRuntimeException("Error reading variant metadata from dataframe", e);
    }

    public static OskarRuntimeException internalException(RuntimeException e) {
        return internalException("Unexpected internal exception: " + e.getMessage(), e);
    }

    public static OskarRuntimeException internalException(String msg, RuntimeException e) {
        return new OskarRuntimeException(msg, e);
    }

    public static OskarRuntimeException missingStudy(Collection<String> studies) {
        return missingParam("study", studies);
    }

    public static OskarRuntimeException missingParam(String paramName, Collection<String> availableValues) {
        if (availableValues != null && !availableValues.isEmpty()) {
            return new OskarRuntimeException("Missing param " + paramName + ". Select one from " + availableValues);
        } else {
            return new OskarRuntimeException("Missing param " + paramName + ".");
        }
    }

    public static OskarRuntimeException unknownStudy(String study, Collection<String> studies) {
        return new OskarRuntimeException("Unknown study '" + study + "'. Select one from " + studies);
    }

    public static OskarRuntimeException unknownFamily(String studyId, String family, Collection<String> families) {
        return unknownFromStudy(studyId, "family", family, families);
    }

    public static OskarRuntimeException unknownSample(String studyId, String sample, Collection<String> samples) {
        return unknownFromStudy(studyId, "sample", sample, samples);
    }

    private static OskarRuntimeException unknownFromStudy(String studyId, String resource, String value,
                                                          Collection<String> availableValues) {
        if (availableValues != null && !availableValues.isEmpty()) {
            return new OskarRuntimeException("Unknown " + resource + " '" + value + "' from study '" + studyId + "'. "
                    + "Select one from " + availableValues);
        } else {
            return new OskarRuntimeException("Unknown " + resource + " '" + value + "' from study '" + studyId + "'.");
        }
    }
}
