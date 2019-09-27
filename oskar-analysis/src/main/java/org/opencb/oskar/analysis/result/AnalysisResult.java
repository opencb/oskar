package org.opencb.oskar.analysis.result;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class AnalysisResult {

    private String id;
    private String executorId;
    private ObjectMap executorParams;
    private Date start;
    private Date end;
    private Status status;
    private List<String> warnings;
    private List<FileResult> outputFiles;
    private List<AnalysisStep> steps;

    private ObjectMap attributes;

    public AnalysisResult() {
        executorParams = new ObjectMap();
        status = new Status();
        warnings = new LinkedList<>();
        outputFiles = new LinkedList<>();
        steps = new LinkedList<>();
        attributes = new ObjectMap();
    }

    public String getId() {
        return id;
    }

    public AnalysisResult setId(String id) {
        this.id = id;
        return this;
    }

    public String getExecutorId() {
        return executorId;
    }

    public AnalysisResult setExecutorId(String executorId) {
        this.executorId = executorId;
        return this;
    }

    public ObjectMap getExecutorParams() {
        return executorParams;
    }

    public AnalysisResult setExecutorParams(ObjectMap executorParams) {
        this.executorParams = executorParams;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public AnalysisResult setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public AnalysisResult setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public AnalysisResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public AnalysisResult setWarnings(List<String> warnings) {
        this.warnings = warnings;
        return this;
    }

    public List<FileResult> getOutputFiles() {
        return outputFiles;
    }

    public AnalysisResult setOutputFiles(List<FileResult> outputFiles) {
        this.outputFiles = outputFiles;
        return this;
    }

    public List<AnalysisStep> getSteps() {
        return steps;
    }

    public AnalysisResult setSteps(List<AnalysisStep> steps) {
        this.steps = steps;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AnalysisResult setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
