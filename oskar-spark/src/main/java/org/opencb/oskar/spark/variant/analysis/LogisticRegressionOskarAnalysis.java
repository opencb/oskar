package org.opencb.oskar.spark.variant.analysis;

import org.opencb.oskar.analysis.variant.VariantOskarAnalysis;
import org.opencb.oskar.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/05/17.
 */
public class LogisticRegressionOskarAnalysis extends VariantOskarAnalysis {

    private String depVarName;
    private String indepVarName;

    private int numIterations = 10; // number of iterations
    private double regularization = 0.3; // regularization parameter
    private double elasticNet = 0.8; // elastic net mixing parameter

    @Override
    public void execute() {
//        LogisticRegression lr = new LogisticRegression()
//                .setMaxIter(numIterations)
//                .setRegParam(regularization)
//                .setElasticNetParam(elasticNet);
//
//        // prepare dataset
//        Dataset<Row> training = null;
//
//        // fit the model
//        LogisticRegressionModel lrModel = lr.fit(training);
//
//        // print the coefficients and intercept for linear regression
//        System.out.println("Coefficients: "
//                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
//
//        // summarize the model over the training set and print out some metrics
//        LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();
//        System.out.println("numIterations: " + trainingSummary.totalIterations());
//        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
//
//        // obtain the loss per iteration
//        double[] objectiveHistory = trainingSummary.objectiveHistory();
//        for (double lossPerIteration : objectiveHistory) {
//            System.out.println(lossPerIteration);
//        }
//
//        // obtain the metrics useful to judge performance on test data
//        // we cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
//        // classification problem.
//        BinaryLogisticRegressionSummary binarySummary =
//                (BinaryLogisticRegressionSummary) trainingSummary;
//
//        // obtain the receiver-operating characteristic as a dataframe and areaUnderROC
//        Dataset<Row> roc = binarySummary.roc();
//        roc.show();
//        roc.select("FPR").show();
//        System.out.println(binarySummary.areaUnderROC());
//
//        // get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
//        // this selected threshold
//        Dataset<Row> fMeasure = binarySummary.fMeasureByThreshold();
//        double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
//        double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
//                .select("threshold").head().getDouble(0);
//        lrModel.setThreshold(bestThreshold);
    }

    public LogisticRegressionOskarAnalysis(String studyId, String depVarName, String indepVarName, OskarConfiguration configuration) {
        this(studyId, depVarName, indepVarName, 10, 0.3, 0.8, configuration);
    }

    public LogisticRegressionOskarAnalysis(String studyId, String depVarName, String indepVarName,
                                           int numIterations, double regularization, double elasticNet, OskarConfiguration configuration) {
        super(studyId, configuration);
        this.depVarName = depVarName;
        this.indepVarName = indepVarName;
        this.numIterations = numIterations;
        this.regularization = regularization;
        this.elasticNet = elasticNet;
    }

    public String getDepVarName() {
        return depVarName;
    }

    public void setDepVarName(String depVarName) {
        this.depVarName = depVarName;
    }

    public String getIndepVarName() {
        return indepVarName;
    }

    public void setIndepVarName(String indepVarName) {
        this.indepVarName = indepVarName;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public void setNumIterations(int numIterations) {
        this.numIterations = numIterations;
    }

    public double getRegularization() {
        return regularization;
    }

    public void setRegularization(double regularization) {
        this.regularization = regularization;
    }

    public double getElasticNet() {
        return elasticNet;
    }

    public void setElasticNet(double elasticNet) {
        this.elasticNet = elasticNet;
    }
}
