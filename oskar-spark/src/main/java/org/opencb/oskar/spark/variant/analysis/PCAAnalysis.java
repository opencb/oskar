package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.opencb.oskar.analysis.variant.VariantAnalysisExecutor;
import org.opencb.oskar.core.config.OskarConfiguration;

/**
 * Created by jtarraga on 30/05/17.
 */
public class PCAAnalysis extends VariantAnalysisExecutor {

    private int kValue = 3;
    private String featureName;

    @Override
    public void execute() {
        // prepare dataset
        Dataset<Row> dataset = null;

        // fit PCA
//        PCAModel pca = new PCA()
//                .setInputCol(featureName)
//                .setOutputCol("pca")
//                .setK(kValue)
//                .fit(dataset);
//
//        Dataset<Row> result = pca.transform(dataset).select("pca");
//        result.show(false);
    }

    public PCAAnalysis(String studyId, String studyName, String featureName, OskarConfiguration configuration) {
        this(studyId, studyName, featureName, 3, configuration);
    }

    public PCAAnalysis(String studyId, String studyName, String featureName, int kValue, OskarConfiguration configuration) {
        super(studyId, configuration);
        this.featureName = featureName;
        this.kValue = kValue;
    }

    public int getkValue() {
        return kValue;
    }

    public void setkValue(int kValue) {
        this.kValue = kValue;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }
}
