import unittest

from pyoskar.sql import *
from pyoskar_tests.test_utils import *

LIMIT = 3


class TestUdfs(TestOskarBase):

    def test_revcomp(self):
        self.df.select("alternate", revcomp("alternate")).show(LIMIT)

    def test_study(self):
        self.df.select(study("studies", "hgvauser@platinum:illumina_platinum")).show(1, truncate=False)

    def test_file(self):
        self.df.select(file("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").alias("platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).show(LIMIT, truncate=False)

    def test_file_attribute(self):
        self.df.select(file_attribute("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz", "DP").alias("DP")).where("DP is not null").show(LIMIT)

    def test_file_filter(self):
        self.df.select(file_filter("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).show(LIMIT, truncate=False)

    def test_file_qual(self):
        self.df.select(file_qual("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).show(LIMIT)

    def test_genotype(self):
        self.df.select(self.df.id, genotype("studies", "NA12877")).show(LIMIT)

    def test_sample_data(self):
        self.df.select(sample_data("studies", 'NA12877').alias("NA12877")).show(LIMIT)

    def test_sample_data_field(self):
        self.df.selectExpr("sample_data_field(studies, 'NA12877', 'GT') as NA12877").show(LIMIT)

    def test_genes(self):
        self.df.select(genes("annotation").alias("genes")).where("genes[0] is not null").show(LIMIT)

    def test_ensembl_genes(self):
        self.df.select(ensembl_genes("annotation").alias("genes")).where("genes[0] is not null ").show(LIMIT)

    def test_consequence_types(self):
        self.df.select(self.df.id, consequence_types("annotation").alias("CT")).show(LIMIT)

    def test_consequence_types_by_gene(self):
        self.df.select(self.df.id, consequence_types_by_gene("annotation", "NBEAP3").alias("CT")).show(LIMIT)

    def test_biotypes(self):
        self.df.select(self.df.id, biotypes("annotation")).show(LIMIT)

    def test_protein_substitution(self):
        self.df.select(self.df.id, protein_substitution("annotation", "polyphen").alias("polyphen"), protein_substitution("annotation", "sift")
                       .alias("sift")).where("polyphen[0]>=0").show(LIMIT)

    def test_functional(self):
        self.df.select(self.df.id, functional("annotation", "cadd_scaled")).show(LIMIT)

    def test_conservation_phastCons(self):
        self.df.select(conservation("annotation", "phastCons").alias("phastCons")).show(LIMIT)

    def test_conservation_gerp(self):
        self.df.select(conservation("annotation", "gerp").alias("gerp")).filter("gerp is not null").show(LIMIT)

    def test_population_frequency(self):
        self.df.select(self.df.id, population_frequency("annotation", "GNOMAD_GENOMES", "ALL")).show(LIMIT)

    def test_population_frequency_as_map(self):
        self.df.select(self.df.id, population_frequency_as_map("annotation")).show(LIMIT)


if __name__ == '__main__':
    unittest.main()
