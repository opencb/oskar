from pyspark.sql.dataframe import DataFrame
from pyoskar.spark.analysis import *

__all__ = ['Oskar']


class Oskar(JavaWrapper):

    def __init__(self, spark):
        super(Oskar, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.Oskar", spark._jsparkSession)
        self.spark = spark
        self.metadata = VariantMetadataManager()

    def load(self, file_path):
        """
        :type file_path: str
        """
        df = self._call_java("load", file_path)

        return df

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


class VariantMetadataManager(JavaWrapper):

    def __init__(self):
        super(VariantMetadataManager, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.VariantMetadataManager")

    def readMetadata(self, meta_path):
        """

        :type meta_path: str
        :param meta_path: Path to the metadata file
        :return: An instance of VariantMetadata
        """
        return self._call_java("readMetadata", meta_path)

    def samples(self, df, studyId = None):
        if studyId is None:
            return self._call_java("samples", df)
        else:
            return self._call_java("samples", df, studyId)

    def variantMetadata(self, df):
        java_vm = self._call_java("variantMetadata", df)
        json_vm = java_vm.toString()
        import json
        return json.loads(json_vm)

    def pedigrees(self, df, studyId = None):
        import json
        if studyId is None:
            pedigrees_dict = {}
            java_vm = self._call_java("pedigrees", df)
            it = java_vm.entrySet().iterator()
            while it.hasNext():
                entry = it.next()
                studyId = entry.getKey()
                pedigrees = entry.getValue()
                pedigrees_dict[studyId] = []
                for i in range(0, pedigrees.size()):
                    pedigrees_dict[studyId].append(json.loads(pedigrees.get(i).toJSON()))
            return pedigrees_dict
        else:
            java_vm = self._call_java("pedigrees", df, studyId)
            pedigrees = []
            for i in range(0, java_vm.size()):
                pedigrees.append(json.loads(java_vm.get(i).toJSON()))
            return pedigrees


class OskarException(Exception):
    def __init__(self, *args, **kwargs):
        super(OskarException, self).__init__(*args, **kwargs)

