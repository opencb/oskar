package org.opencb.oskar.analysis.variant;

public class TdtTestResult {
    private double chiSquare;
    private double oddRatio;
    private double pValue;
    private int df; // Degrees of freedom
    private int t1; // Transmission count 1
    private int t2; // Transmission count 2

    public TdtTestResult(double chiSquare, double oddRatio, double pValue, int df, int t1, int t2) {
        this.chiSquare = chiSquare;
        this.oddRatio = oddRatio;
        this.pValue = pValue;
        this.df = df;
        this.t1 = t1;
        this.t2 = t2;
    }
}
