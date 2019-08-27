package org.opencb.oskar.analysis.stats;

import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.security.InvalidParameterException;


public class CMC {

    private CMCResult result = new CMCResult();

    /**
     * The CMC method is a pooling approach proposed by Li and Leal (2008) that uses allele frequencies
     * to determine the partition of the variants into groups. After the rare variants are selected, they are
     * collapsed into an indicator variable, and then a multivariate test such as Hotelling’s T2 test is applied
     * to the collection formed by the common variants and the collapsed super-variant.Compute the CMC test.
     *
     * Based on the AssotesteR code
     * https://github.com/gastonstat/AssotesteR/blob/master/R/CMC.R
     *
     * @param phenotype         Vector with phenotype status: 0 = controls, 1 = cases
     * @param genotype          Matrix with genotype data coded as 0, 1, 2.
     * @return                  CMC result
     */
    public CMCResult run(RealVector phenotype, RealMatrix genotype) {
        return run(phenotype, genotype, 0.05, 100);
    }

    /**
     * The CMC method is a pooling approach proposed by Li and Leal (2008) that uses allele frequencies
     * to determine the partition of the variants into groups. After the rare variants are selected, they are
     * collapsed into an indicator variable, and then a multivariate test such as Hotelling’s T2 test is applied
     * to the collection formed by the common variants and the collapsed super-variant.Compute the CMC test.
     *
     * Based on the AssotesteR code
     * https://github.com/gastonstat/AssotesteR/blob/master/R/CMC.R
     *
     * @param phenotype         Vector with phenotype status: 0 = controls, 1 = cases
     * @param genotype          Matrix with genotype data coded as 0, 1, 2.
     * @param maf               Minor allele frequency threshold for rare variations
     * @param numPermutations   Number of permutations
     * @return                  CMC result
     */
    public CMCResult run(RealVector phenotype, RealMatrix genotype, double maf, int numPermutations) {
//        System.out.println("phenotype:\n" + phenotype.toString());
//        System.out.println("genotype:\n" + genotype.toString());

        result.setMaf(maf);
        result.setNumPermutations(numPermutations);

        // number of variants
        int numVariants = genotype.getColumnDimension();
//        System.out.println("number of variants = " + numVariants);
        result.setNumVariants(numVariants);

        // number of individuals
        int numIndividuals = phenotype.getDimension();
//        System.out.println("number of individuals = " + numIndividuals);

        // get minor allele frequencies
        int rare = 0;
        ArrayRealVector mafArray = new ArrayRealVector(numVariants);
        boolean[] isRare = new boolean[numVariants];
        for (int i = 0; i < numIndividuals; i++) {
            // sanity check
            if (numVariants != genotype.getRow(i).length) {
                throw new InvalidParameterException("Number of variants mismatch!");
            }
            for (int j = 0; j < numVariants; j++) {
                mafArray.setEntry(j, mafArray.getEntry(j) + genotype.getEntry(i, j));
            }
        }
//        System.out.println("MAF, isRare:");
        for (int j = 0; j < numVariants; j++) {
            mafArray.setEntry(j, mafArray.getEntry(j) / numIndividuals / 2.0);
            if (mafArray.getEntry(j) < maf) {
                isRare[j] = true;
                rare++;
            } else {
                isRare[j] = false;
            }
//            System.out.print(MAF.getEntry(j) + "=" + isRare[j] + "\t");
        }
//        System.out.println();
        result.setNumRareVariants(rare);

        // collapsing
        RealMatrix newGenotype;
        if (rare <= 1) {
            // if rare variants <= 1, then NO collapse is needed
            newGenotype = new Array2DRowRealMatrix(genotype.getData());
        } else {
            // collapsing rare variants into one column
            // X.collaps = rowSums(X[,rare.maf], na.rm=TRUE)
            double sum;
            double[] collapse = new double[numIndividuals];
            for (int i = 0; i < numIndividuals; i++) {
                sum = 0;
                double[] row = genotype.getRow(i);
                for (int j = 0; j < row.length; j++) {
                    if (isRare[j]) {
                        sum += row[j];
                    }
                }
                // X.collaps[X.collaps != 0] = 1
                collapse[i] = (sum != 0 ? 1 : 0);
            }
//            System.out.println("Collapse: " + collapse);

            // joining collapsed to common variants
            // X.new = cbind(X[,!rare.maf], X.collaps)
            newGenotype = new Array2DRowRealMatrix(new double[numIndividuals][numVariants - rare + 1]);
//            System.out.println("newGenotype, num. rows = " + newGenotype.getRowDimension()
//                    + ", num. columns = " + newGenotype.getColumnDimension());

            int col = 0;
            for (int j = 0; j < numVariants; j++) {
                if (!isRare[j]) {
                    newGenotype.setColumn(col, genotype.getColumn(j));
                    col++;
                }
            }
            newGenotype.setColumn(col, collapse);
        }
//        System.out.println("newGenotype after collapsing:\n" + newGenotype.toString());

        // change values to -1, 0, 1
        // X.new = X.new - 1
        newGenotype = newGenotype.scalarAdd(-1.0);
//        System.out.println("newGenotype after changing values to -1, 0, 1:\n" + newGenotype.toString());

        // number of new variants
        // M = ncol(X.new)
        int numNewVariants = newGenotype.getColumnDimension();

        // Hotellings T2 statistic
        double stat = computeStatistic(phenotype, newGenotype);
        result.setStatistic(stat);
        //result.setStatistic(2.666667);

        // Asymptotic p-values
        // under the null hypothesis T2 follows an F distribution
        // f.stat = cmc.stat * (N-M-1)/(M*(N-2))
        double fStat = stat * (numIndividuals - numNewVariants - 1) / (numNewVariants * (numIndividuals - 2));

        // degrees of freedom
        // df1 = M
        int df1 = numNewVariants;

        // degrees of freedom
        // df2 = N - M - 1
        int df2 = numIndividuals - numNewVariants - 1;

        // asym.pval = 1 - pf(f.stat, df1, df2)
        FDistribution fDistribution = new FDistribution(df1, df2);
        result.setAsymPvalue(1 - fDistribution.cumulativeProbability(fStat));

        // permutations
        double newStat;
        int counter = 0;
        RealVector newPhenotype = new ArrayRealVector(phenotype.getDimension());
        if (numPermutations > 0)  {
            //double[] perm = new double[numPermutations];
            RandomDataGenerator random = new RandomDataGenerator();
            for (int i = 0; i < numPermutations; i++) {
                int[] sample = random.nextPermutation(numIndividuals, numIndividuals);
                for (int j = 0; j < sample.length; j++) {
                    newPhenotype.setEntry(j, phenotype.getEntry(sample[j]));
                }
                newStat = computeStatistic(newPhenotype, newGenotype);
                if (newStat > stat) {
                    counter++;
                }
            }
            // p-value
            // perm.pval = sum(x.perm > cmc.stat) / perm
            result.setPermPvalue(1.0 * counter / numPermutations);
        }

        return  result;
    }

    /**
     * The CMC method is a pooling approach proposed by Li and Leal (2008) that uses allele frequencies
     * to determine the partition of the variants into groups. After the rare variants are selected, they are
     * collapsed into an indicator variable, and then a multivariate test such as Hotelling’s T2 test is applied
     * to the collection formed by the common variants and the collapsed super-variant.Compute the CMC test.
     *
     * Based on the AssotesteR code
     * https://github.com/gastonstat/AssotesteR/blob/master/R/AssotesteR-internal.R
     *
     * @param phenotype         Vector with phenotype status: 0 = controls, 1 = cases
     * @param genotype          Matrix with genotype data coded as 0, 1, 2.
     * @return                  Statistic
     */
    private double computeStatistic(RealVector phenotype, RealMatrix genotype) {
        double stat = 0.0;

        // phenotype = casecons, genotype = X
        // number of individuals N, cases nA, controls nU
        // N = nrow(X.new)
        int numIndividuals = genotype.getRowDimension();

        // nA = sum(casecon)
        int nA = 0;
        for (int i = 0; i < phenotype.getDimension(); i++) {
            nA += phenotype.getEntry(i);
        }
        result.setNumCases(nA);

        // nU = N - nA
        int nU = numIndividuals - nA;
        result.setNumControls(nU);
//        System.out.println(N + " individuals, " + nA + " cases, " + nU + " controls");

        // matrix of genotypes in cases
        // Xx = X.new[casecon==1,]
        // matrix of genotypes in controls
        // Yy = X.new[casecon==0,]
        int xRow = 0;
        int yRow = 0;
        RealMatrix xX = new Array2DRowRealMatrix(nA, genotype.getColumnDimension());
        RealMatrix yY = new Array2DRowRealMatrix(nU, genotype.getColumnDimension());
        for (int row = 0; row < phenotype.getDimension(); row++) {
            if (phenotype.getEntry(row) == 1) {
                // case
                xX.setRow(xRow++, genotype.getRow(row));
            } else {
                // control
                yY.setRow(yRow++, genotype.getRow(row));
            }
        }
//        System.out.println("Xx = " + Xx);
//        System.out.println("Yy = " + Yy);

        // get means
        // Xx.mean = colMeans(Xx, na.rm=TRUE)
        double[] xXMean = colMeans(xX);
//        System.out.print("xXMean = ");
//        for (int i = 0; i < xXMean.length; i++) {
//            System.out.print(xXMean[i] + "\t");
//        }
//        System.out.println();

        // Yy.mean = colMeans(Yy, na.rm=TRUE)
        double[] yYMean = colMeans(yY);
//        System.out.print("yYMean = ");
//        for (int i = 0; i < yYMean.length; i++) {
//            System.out.print(yYMean[i] + "\t");
//        }
//        System.out.println();

        // center matrices Xx and Yy
        // Dx = sweep(Xx, 2, Xx.mean)
        // Dy = sweep(Yy, 2, Yy.mean)
        RealMatrix dX = substractVector(xX, xXMean);
//        System.out.println("Dx = " + Dx);

        RealMatrix dY = substractVector(yY, yYMean);
//        System.out.println("Dy = " + Dy);

        // pooled covariance matrix
        // assuming no missing values
        // COV = (t(Dx) %*% Dx + t(Dy) %*% Dy) / (N-2)
        RealMatrix cov = dX.transpose().multiply(dX).add(dY.transpose().multiply(dY)).scalarMultiply(1.0 / (numIndividuals - 2));
//        System.out.println("COV = " + COV);

        // general inverse
        RealMatrix invCov;
        if (cov.getRowDimension() == 1) {
            // only one variant
            // if (COV < 1e-8) COV = 1e-8
            for (int row = 0; row < cov.getRowDimension(); row++) {
                for (int col = 0; col < cov.getColumnDimension(); col++) {
                    if (cov.getEntry(row, col) < 1e-8) {
                        cov.setEntry(row, col, 1e-8);
                    }
                }
            }

            // COV.inv = 1 / COV
            invCov = new LUDecomposition(cov).getSolver().getInverse();
        } else {
            // COV.eigen = eigen(COV)
            EigenDecomposition eigenCOV = new EigenDecomposition(cov);

            // eig.vals = COV.eigen$values
            double[] eigVals = eigenCOV.getRealEigenvalues();
//            System.out.print("eigVals: ");
//            for (int i = 0; i < eigVals.length; i++) {
//                System.out.print(eigVals[i] + "\t");
//            }
//            System.out.println();

            // inv.vals = ifelse(abs(eig.vals) <= 1e-8, 0, 1/eig.vals)
            double[] invEigVals = new double[eigVals.length];
            for (int i = 0; i < eigVals.length; i++) {
                invEigVals[i] = (Math.abs(eigVals[i]) <= 1e-8 ? 0 : 1.0 / eigVals[i]);
            }
//            System.out.print("invEigVals: ");
//            for (int i = 0; i < eigVals.length; i++) {
//                System.out.print(eigVals[i] + "\t");
//            }
//            System.out.println();

            // EV = solve(COV.eigen$vectors)
            RealMatrix eigenVectors = eigenCOV.getV();
            DecompositionSolver solver = new LUDecomposition(eigenVectors).getSolver();
            RealMatrix constants = new Array2DRowRealMatrix(eigenVectors.getRowDimension(),
                    eigenVectors.getColumnDimension());
            for (int i = 0; i < constants.getRowDimension(); i++) {
                constants.setEntry(i, i, 1.0);
            }
            RealMatrix eV = solver.solve(constants);
//            System.out.println("EV: " + EV);

            // COV.inv = t(EV) %*% diag(inv.vals) %*% EV
            RealMatrix diag = new Array2DRowRealMatrix(cov.getRowDimension(), cov.getColumnDimension());
            for (int i = 0; i < invEigVals.length; i++) {
                diag.setEntry(i, i, invEigVals[i]);
            }
//            System.out.println("diag: " + diag);
            invCov = eV.transpose().multiply(diag).multiply(eV);
//            System.out.println("invCOV: " + invCOV);
        }

        // Hotellings T2 statistic
        // stat = t(Xx.mean - Yy.mean) %*% COV.inv %*% (Xx.mean - Yy.mean) * nA * nU / N
        RealVector xXMeanVector = new ArrayRealVector(xXMean);
        RealVector yYMeanVector = new ArrayRealVector(yYMean);
        RealMatrix diff = new Array2DRowRealMatrix(xXMeanVector.getDimension(), 1);
        diff.setColumn(0, xXMeanVector.subtract(yYMeanVector).toArray());
        RealMatrix statMatrix = diff.transpose().multiply(invCov).multiply(diff).scalarMultiply(1.0 * nA * nU / numIndividuals);
//        System.out.println("statMatrix = " + statMatrix);

        return statMatrix.getEntry(0, 0);
    }

    /**
     * Compute the column means.
     *
     * @param matrix    Matrix target
     * @return          Array with the column means
     */
    private double[] colMeans(RealMatrix matrix) {
        double sum;
        double[] column;
        double[] mean = new double[matrix.getColumnDimension()];
        for (int i = 0; i < matrix.getColumnDimension(); i++) {
            sum = 0;
            column = matrix.getColumn(i);
            for (int j = 0; j < column.length; j++) {
                sum += column[j];
            }
            mean[i] = sum / column.length;
        }
        return mean;
    }

    /**
     * Substrat vector to the matrix column.
     *
     * @param matrix    Matrix target
     * @param vector    Vector to substract
     * @return          Matrix
     */
    private RealMatrix substractVector(RealMatrix matrix, double[] vector) {
        RealMatrix res = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension());
        for (int row = 0; row < res.getRowDimension(); row++) {
            for (int col = 0; col < res.getColumnDimension(); col++) {
                res.setEntry(row, col, matrix.getEntry(row, col) - vector[col]);
            }
        }
        return res;
    }
}
