package org.opencb.oskar.analysis.result;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Date;

public class AnalysisStep {

    private String id;
    private Date start;
    private Date end;
    private Status status;
    private ObjectMap attributes;

    public AnalysisStep() {
        attributes = new ObjectMap();
        status = new Status();
    }

    public AnalysisStep(String id, Date start, Date end, Status status, ObjectMap attributes) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.status = status;
        this.attributes = attributes;
    }

    public String getId() {
        return id;
    }

    public AnalysisStep setId(String id) {
        this.id = id;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public AnalysisStep setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public AnalysisStep setEnd(Date end) {
        this.end = end;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public AnalysisStep setStatus(Status status) {
        this.status = status;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public AnalysisStep setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
