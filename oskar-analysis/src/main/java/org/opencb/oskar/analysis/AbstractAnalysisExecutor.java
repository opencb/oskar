package org.opencb.oskar.analysis;

import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class AbstractAnalysisExecutor {

    public abstract AnalysisResult exec() throws AnalysisException;

    public String getDateTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }
}
