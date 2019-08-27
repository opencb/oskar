package org.opencb.oskar.analysis.stats;


public class FisherTestResult extends AbstractTestResult {

    private double pValue;
    private double oddRatio;

    public FisherTestResult(double pValue, double oddRatio) {
        this.pValue = pValue;
        this.oddRatio = oddRatio;
    }

    @Override
    public String[] getColumns() {
        return new String[]{"ODD_RATIO", "PVALUE"};
    }

    @Override
    public String getResult() {
        return this.oddRatio + "\t" + this.pValue;
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
