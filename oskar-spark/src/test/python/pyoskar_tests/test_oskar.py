from unittest import main

from pyoskar.sql import *
from pyoskar_tests.test_utils import *


class TestOskar(TestOskarBase):

    def test_load(self):
        self.df.show()

    def test_samples(self):
        samples = self.oskar.metadata.samples(self.df)
        assert len(samples["hgvauser@platinum:illumina_platinum"]) == 17
        for i in range(0, 17):
            assert samples["hgvauser@platinum:illumina_platinum"][i] == "NA" + str(12877 + i)

    def test_filter_study(self):
        self.df.selectExpr("studies[0].files.fileId").show(10, False)

        self.df.select(file_qual("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").alias("qual")).show(10)
        self.df.selectExpr("file_qual(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz')").show(10)

    def test_filter_annot(self):
        self.df.withColumn("freqs", population_frequency_as_map("annotation")).where("freqs['1kG_phase3:ALL'] > 0").show(
            10)
        self.df.select(genes("annotation").alias("genes")).where("genes[0] is not null").show(10)
        self.df.withColumn("freqs", population_frequency_as_map("annotation")).show(100)
        self.df.withColumn("freqs", population_frequency_as_map("annotation")).where("freqs['1kG_phase3:ALL'] > 0").show(
            100)


if __name__ == '__main__':
    main()
