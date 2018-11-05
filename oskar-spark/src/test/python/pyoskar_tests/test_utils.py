from os import path

from pyspark.sql import SparkSession

here = path.abspath(path.dirname(__file__))

TARGET_PATH = here + "/../../../../target/"
RESOURCES_PATH = here + "/../../resources/"
PLATINUM_CHR__SMALL_AVRO = RESOURCES_PATH + "platinum_chr22.small.avro"


def create_testing_pyspark_session():
    return (SparkSession.builder
            .master("local[*]")
            .appName("testing")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", TARGET_PATH + "oskar-spark-0.1.0-jar-with-dependencies.jar")
            .getOrCreate())
