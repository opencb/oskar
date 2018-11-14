package org.opencb.oskar.analysis.variant;


public class FisherTestResult {

    private double pValue;
    private double oddRatio;

    public FisherTestResult(double pValue, double oddRatio) {
        this.pValue = pValue;
        this.oddRatio = oddRatio;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FisherTestResult{");
        sb.append("oddRatio=").append(oddRatio);
        sb.append(", pValue=").append(pValue);
        sb.append('}');
        return sb.toString();
    }

    public double getOddRatio() {
        return oddRatio;
    }

    public FisherTestResult setOddRatio(double oddRatio) {
        this.oddRatio = oddRatio;
        return this;
    }

    public double getpValue() {
        return pValue;
    }

    public FisherTestResult setpValue(double pValue) {
        this.pValue = pValue;
        return this;
    }
}
