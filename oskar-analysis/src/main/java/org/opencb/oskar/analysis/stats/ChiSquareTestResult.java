package org.opencb.oskar.analysis.stats;


public class ChiSquareTestResult {
    private double chiSquare;
    private double pValue;
    private double oddRatio;

    public ChiSquareTestResult(double chiSquare, double pValue, double oddRatio) {
        this.chiSquare = chiSquare;
        this.pValue = pValue;
        this.oddRatio = oddRatio;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ChiSquareTestResult{");
        sb.append("chiSquare=").append(chiSquare);
        sb.append(", pValue=").append(pValue);
        sb.append(", oddRatio=").append(oddRatio);
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

    public double getOddRatio() {
        return oddRatio;
    }

    public ChiSquareTestResult setOddRatio(double oddRatio) {
        this.oddRatio = oddRatio;
        return this;
    }
}
