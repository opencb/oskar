from os import path

from pyspark.sql import SparkSession
from unittest import TestCase
from pyoskar.core import Oskar

here = path.abspath(path.dirname(__file__))

TARGET_PATH = here + "/../../../../target/"
RESOURCES_PATH = here + "/../../resources/"
PLATINUM_SMALL = RESOURCES_PATH + "platinum_chr22.small.parquet"


def create_testing_pyspark_session():
    jar = TARGET_PATH + "oskar-spark-0.1.0-jar-with-dependencies.jar"
    if not path.exists(jar):
        raise Exception("JAR file \"" + jar + "\" not found! Unable to start SparkSession")

    return (SparkSession.builder
            .master("local[*]")
            .appName("testing")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .getOrCreate())


class TestOskarBase(TestCase):
    spark = None  # type: SparkSession
    oskar = None  # type: Oskar
    df = None  # type: DataFrame

    @classmethod
    def setUpClass(cls):
        cls.spark = create_testing_pyspark_session()
        cls.oskar = Oskar(cls.spark)
        cls.df = cls.oskar.load(PLATINUM_SMALL)

    def setUp(self):
        self.spark = self.__class__.spark
        self.oskar = self.__class__.oskar
        self.df = self.__class__.df

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
