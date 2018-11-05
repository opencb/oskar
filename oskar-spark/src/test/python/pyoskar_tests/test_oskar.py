from unittest import TestCase

from pyspark.sql import DataFrame

from pyoskar.analysis.sql import *
from pyoskar.core import Oskar
from pyoskar_tests.test_utils import *


class TestOskar(TestCase):
    spark = None  # type: SparkSession
    oskar = None  # type: Oskar
    df = None  # type: DataFrame

    @classmethod
    def setUpClass(cls):
        cls.spark = create_testing_pyspark_session()
        cls.oskar = Oskar(cls.spark)
        cls.df = cls.oskar.load(PLATINUM_CHR__SMALL_AVRO)

    def setUp(self):
        self.spark = self.__class__.spark
        self.oskar = self.__class__.oskar
        self.df = self.__class__.df

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load(self):
        self.df.show()

    def test_samples(self):
        samples = self.oskar.samples(self.df)
        assert len(samples) == 17
        for i in range(0, 17):
            assert samples[i] == "NA" + str(12877 + i)

    def test_filter_study(self):
        self.df.selectExpr("studies[0].files.fileId").show(10, False)

        self.df.select(fileQual("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").alias("qual")).show(10)
        self.df.selectExpr("fileQual(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz')").show(10)

    def test_filter_annot(self):
        self.df.withColumn("freqs", populationFrequencyAsMap("annotation")).where("freqs['1kG_phase3:ALL'] > 0").show(
            10)
        self.df.select(genes("annotation").alias("genes")).where("genes[0] is not null").show(10)
        self.df.withColumn("freqs", populationFrequencyAsMap("annotation")).show(100)
        self.df.withColumn("freqs", populationFrequencyAsMap("annotation")).where("freqs['1kG_phase3:ALL'] > 0").show(
            100)
