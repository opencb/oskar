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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TdtTestResult{");
        sb.append("chiSquare=").append(chiSquare);
        sb.append(", oddRatio=").append(oddRatio);
        sb.append(", pValue=").append(pValue);
        sb.append(", df=").append(df);
        sb.append(", t1=").append(t1);
        sb.append(", t2=").append(t2);
        sb.append('}');
        return sb.toString();
    }

    public double getChiSquare() {
        return chiSquare;
    }

    public TdtTestResult setChiSquare(double chiSquare) {
        this.chiSquare = chiSquare;
        return this;
    }

    public double getOddRatio() {
        return oddRatio;
    }

    public TdtTestResult setOddRatio(double oddRatio) {
        this.oddRatio = oddRatio;
        return this;
    }

    public double getpValue() {
        return pValue;
    }

    public TdtTestResult setpValue(double pValue) {
        this.pValue = pValue;
        return this;
    }

    public int getDf() {
        return df;
    }

    public TdtTestResult setDf(int df) {
        this.df = df;
        return this;
    }

    public int getT1() {
        return t1;
    }

    public TdtTestResult setT1(int t1) {
        this.t1 = t1;
        return this;
    }

    public int getT2() {
        return t2;
    }

    public TdtTestResult setT2(int t2) {
        this.t2 = t2;
        return this;
    }
}
