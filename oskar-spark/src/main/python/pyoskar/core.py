
from pyspark.sql.dataframe import DataFrame

from pyoskar.spark.analysis import *

__all__ = ['Oskar']


class Oskar(JavaWrapper):

    def __init__(self, spark):
        super(Oskar, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.Oskar")
        """
        :type spark: SparkSession
        """
        from pyoskar.spark.sql import VariantUdfManager
        VariantUdfManager().loadVariantUdfs(spark)
        self.spark = spark

    def load(self, file_path):
        """
        :type file_path: str
        """
        df = None  # type: DataFrame
        if file_path.endswith("avro") or file_path.endswith("avro.gz"):
            # Do not fail if the file extension is "avro.gz"
            self.spark.sparkContext._jsc.hadoopConfiguration().set('avro.mapred.ignore.inputs.without.extension', 'false')

            df = self.spark.read.format("com.databricks.spark.avro").load(file_path)
        elif file_path.endswith("parquet"):
            df = self.spark.read.format("parquet").load(file_path)
        else:
            raise OskarException("Unsupported format for file " + file_path)

        # Read and add metadata
        meta_path = file_path + ".meta.json.gz"

        import os
        if os.path.exists(meta_path):
            df = self.add_variant_metadata(df, meta_path)

        return df

    def add_variant_metadata(self, df, meta_path):
        return self._call_java("addVariantMetadata", df, meta_path)

    def load_metadata(self, meta_path):
        import json
        import gzip
        with gzip.open(meta_path, "rb") as f:
            return json.loads(f.read().decode("ascii"))

    def samples(self, df, study=None):
        samples_ = df.schema["studies"].dataType.elementType["samplesData"].metadata["samples"]
        if study is None:
            if len(samples_) == 1:
                for study in samples_:
                    return samples_[study]
            else:
                raise OskarException("Missing study. Select one from " + samples_.keys())
        else:
            return samples_[study]

    def stats(self, df, studyId=None, cohort="ALL", samples=None):
        """

        :type df: DataFrame
        """
        return VariantStatsTransformer(studyId=studyId, cohort=cohort, samples=samples).transform(df)

    # def sample_stats(self, df, samples, studyId=None):

    def global_stats(self, df, studyId=None, fileId=None):
        """

        :type df: DataFrame
        """
        return VariantSetStatsTransformer(studyId=studyId, fileId=fileId).transform(df)

    def histogram(self, df, inputCol, step):
        """

        :type df: DataFrame
        """
        return HistogramTransformer(step=step, inputCol=inputCol).transform(df)

    def hardy_weinberg(self, df, studyId=None):
        return HardyWeinbergTransformer(studyId=studyId).transform(df)

    def ibs(self, df, skipReference=None, samples=None, numPairs=None):
        """

        :type df: DataFrame
        """
        return IBSTransformer(skipReference=skipReference, samples=samples, numPairs=numPairs).transform(df)

    def mendel(self, df):
        """

        :type df: DataFrame
        """
        return MendelianErrorTransformer().transform(df)

    # def de_novo(self, df):
    # def ld_matrix(self, df):
    # def impute_sex(self, df):
    # def hwe_normalized_pca(self, df):
    # def concordance(self, df):
    # def cancer_signature(self, df): #https://cancer.sanger.ac.uk/cosmic/signatures



class OskarException(Exception):
    def __init__(self, *args, **kwargs):
        super(OskarException, self).__init__(*args, **kwargs)

