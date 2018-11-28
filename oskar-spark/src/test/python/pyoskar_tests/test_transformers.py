import unittest

from pyoskar.spark.sql import *
from pyoskar_tests.test_utils import *
from pyspark.sql.functions import *


LIMIT = 3


class TestTransformers(TestOskarBase):

    def test_chi_square(self):
        self.oskar.chiSquare(self.df, "hgvauser@platinum:illumina_platinum", "KK").show(LIMIT)

    def test_compound_heterozygote(self):
        self.oskar.compoundHeterozygote(self.df, "NA12877", "NA12878", "NA12879", missingGenotypeAsReference=True).show(LIMIT)

    def test_fisher(self):
        self.oskar.fisher(self.df, "hgvauser@platinum:illumina_platinum", "KK").select("id", col("Fisher p-value").alias("fisher")).filter(col("fisher") != 1.0).show(LIMIT)

    def test_hardy_weinberg(self):
        self.oskar.hardyWeinberg(self.df, "hgvauser@platinum:illumina_platinum").show(LIMIT)

    def test_histogram(self):
        self.oskar.histogram(self.df, "start", 5).show(LIMIT)

    def test_ibs(self):
        self.oskar.ibs(self.df).show(LIMIT)

    def test_ibs_full(self):
        self.oskar.ibs(self.df, samples=["NA12877", "NA12878", "NA12879"], skipMultiAllelic=True, skipReference=True).show(LIMIT)

    def test_impute_sex(self):
        self.oskar.imputeSex(self.df).show(LIMIT)

    def test_inbreeding_coefficient(self):
        df2 = self.oskar.stats(self.df, studyId="hgvauser@platinum:illumina_platinum", missingAsReference=True)
        self.oskar.inbreedingCoefficient(df2).show(LIMIT)

    def test_mendelian_error(self):
        self.oskar.mendel(self.df, father="NA12877", mother="NA12878", child="NA12879", studyId="hgvauser@platinum:illumina_platinum").show(LIMIT)

    def test_tdt(self):
        self.oskar.tdt(self.df, "hgvauser@platinum:illumina_platinum", "KK").show(LIMIT)

    def test_stats(self):
        self.oskar.stats(self.df, studyId="hgvauser@platinum:illumina_platinum", missingAsReference=True) \
            .selectExpr("id", "studies[0].stats.ALL as stats").select("id", "stats.*") \
            .select("id", "alleleCount", "altAlleleCount", "missingAlleleCount", "altAlleleFreq", "maf").show(LIMIT, False)

    def test_global_stats(self):
        self.oskar.globalStats(self.df, "hgvauser@platinum:illumina_platinum").show(LIMIT)


if __name__ == '__main__':
    unittest.main()
