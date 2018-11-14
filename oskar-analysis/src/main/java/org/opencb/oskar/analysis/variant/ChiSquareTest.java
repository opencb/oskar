package org.opencb.oskar.analysis.variant;

import java.security.InvalidParameterException;


public class ChiSquareTest {

    public static ChiSquareTestResult chiSquareTest(int[] values) throws InvalidParameterException {
        if (values == null || values.length != 4) {
            throw new InvalidParameterException("The matrix must have 4 columns (a, b, c and d)");
        }
        return chiSquareTest(values[0], values[1], values[2], values[3]);
    }

    public static ChiSquareTestResult chiSquareTest(int a, int b, int c, int d) {
        org.apache.commons.math3.stat.inference.ChiSquareTest test =
                new org.apache.commons.math3.stat.inference.ChiSquareTest();
        long[][] matrix = new long[][]{{a, b}, {c, d}};
        return new ChiSquareTestResult(test.chiSquare(matrix), test.chiSquareTest(matrix),
                FisherExactTest.computeOddRatio(a, b, c, d));
    }

}
