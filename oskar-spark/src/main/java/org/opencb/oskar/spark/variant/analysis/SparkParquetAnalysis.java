package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.SparkSession;
import org.opencb.commons.datastore.core.ObjectMap;

public interface SparkParquetAnalysis {

    ObjectMap getParams();

    default SparkSession getSparkSession(String appName) {
        // Prepare input dataset from the input parquet file
        return SparkSession.builder()
                .master(getSparkMaster())
                .appName(appName)
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    default String getSparkMaster() {
        return getParams().getString("MASTER");
    }

    default void setSparkMaster(String master) {
        getParams().put("MASTER", master);
    }

    default String getFile() {
        return getParams().getString("FILE");
    }

    default void setFile(String file) {
        getParams().put("FILE", file);
    }
}
