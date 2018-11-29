import unittest

from pyoskar.spark.sql import *
from pyoskar_tests.test_utils import *
from pyspark.sql.functions import *


LIMIT = 3


class TestMetadata(TestOskarBase):

    def test_metadata_path(self):
        print(self.oskar.metadata.getMetadataPath("metadata"))

    def test_samples(self):
        print(self.oskar.metadata.samples(self.df, "hgvauser@platinum:illumina_platinum"))

    def test_variant_metadata(self):
        print(self.oskar.metadata.variantMetadata(self.df))

    def test_set_variant_metadata(self):
        metadata = self.oskar.metadata.variantMetadata(self.df)
        self.oskar.metadata.setVariantMetadata(self.df, metadata)

    def test_pedigrees(self):
        print(self.oskar.metadata.pedigrees(self.df))

    def test_pedigrees_with_study(self):
        print(self.oskar.metadata.pedigrees(self.df, "hgvauser@platinum:illumina_platinum"))


if __name__ == '__main__':
    unittest.main()
