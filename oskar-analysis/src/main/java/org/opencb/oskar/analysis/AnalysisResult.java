package org.opencb.oskar.analysis;

import org.opencb.commons.datastore.core.ObjectMap;

import java.nio.file.Path;
import java.util.List;

public class AnalysisResult {
    public enum FileType {
        IMAGE,
        JSON,
        TAB_SEPARATED // First line starts with # and contains the header
    }

    String dateTime;
    String analysisId;
    ObjectMap executorParams;
    List<File> outputFiles;
    long executionTime;

    public static class File {
        Path path;
        FileType type;

        public File(Path path, FileType type) {
            this.path = path;
            this.type = type;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("File{");
            sb.append("path=").append(path);
            sb.append(", type=").append(type);
            sb.append('}');
            return sb.toString();
        }

        public Path getPath() {
            return path;
        }

        public File setPath(Path path) {
            this.path = path;
            return this;
        }

        public FileType getType() {
            return type;
        }

        public File setType(FileType type) {
            this.type = type;
            return this;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResult{");
        sb.append("dateTime='").append(dateTime).append('\'');
        sb.append(", analysisId='").append(analysisId).append('\'');
        sb.append(", executorParams=").append(executorParams);
        sb.append(", outputFiles=").append(outputFiles);
        sb.append(", executionTime=").append(executionTime);
        sb.append('}');
        return sb.toString();
    }

    public String getDateTime() {
        return dateTime;
    }

    public AnalysisResult setDateTime(String dateTime) {
        this.dateTime = dateTime;
        return this;
    }

    public String getAnalysisId() {
        return analysisId;
    }

    public AnalysisResult setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
        return this;
    }

    public ObjectMap getExecutorParams() {
        return executorParams;
    }

    public AnalysisResult setExecutorParams(ObjectMap executorParams) {
        this.executorParams = executorParams;
        return this;
    }

    public List<File> getOutputFiles() {
        return outputFiles;
    }

    public AnalysisResult setOutputFiles(List<File> outputFiles) {
        this.outputFiles = outputFiles;
        return this;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public AnalysisResult setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
        return this;
    }
}
