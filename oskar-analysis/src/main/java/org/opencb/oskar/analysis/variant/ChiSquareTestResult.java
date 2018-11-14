package org.opencb.oskar.analysis.variant;


public class ChiSquareTestResult {
    private double chiSquare;
    private double pValue;

    public ChiSquareTestResult(double chiSquare, double pValue) {
        this.chiSquare = chiSquare;
        this.pValue = pValue;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ChiSquareTestResult{");
        sb.append(", chiSquare=").append(chiSquare);
        sb.append(", pValue=").append(pValue);
        sb.append('}');
        return sb.toString();
    }

    public double getChiSquare() {
        return chiSquare;
    }

    public ChiSquareTestResult setChiSquare(double chiSquare) {
        this.chiSquare = chiSquare;
        return this;
    }

    public double getpValue() {
        return pValue;
    }

    public ChiSquareTestResult setpValue(double pValue) {
        this.pValue = pValue;
        return this;
    }
}
