import json
from pyspark.sql.dataframe import DataFrame
from pyoskar.spark.analysis import *

__all__ = ['Oskar']


class Oskar(JavaWrapper):

    def __init__(self, spark):
        super(Oskar, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.Oskar", spark._jsparkSession)
        self.spark = spark
        self.metadata = VariantMetadataManager()
        self.pythonUtils = PythonUtils()

    def load(self, file_path):
        """
        :type file_path: str
        """
        df = self._call_java("load", file_path)
        return df

    def stats(self, df, studyId=None, cohort="ALL", samples=None, missingAsReference=False):
        """

        :type df: DataFrame
        """
        return VariantStatsTransformer(studyId=studyId, cohort=cohort, samples=samples, missingAsReference=missingAsReference).transform(df)

    # def sample_stats(self, df, samples, studyId=None):

    def globalStats(self, df, studyId=None, fileId=None):
        """

        :type df: DataFrame
        """
        return VariantSetStatsTransformer(studyId=studyId, fileId=fileId).transform(df)

    def histogram(self, df, inputCol, step):
        """

        :type df: DataFrame
        """
        return HistogramTransformer(step=step, inputCol=inputCol).transform(df)

    def hardyWeinberg(self, df, studyId=None):
        return HardyWeinbergTransformer(studyId=studyId).transform(df)

    def ibs(self, df, samples=None, skipMultiAllelicParam=None, skipReference=None, numPairs=None):
        """

        :type df: DataFrame
        """
        transformer = IBSTransformer()
        if samples is not None:
            transformer.setSamples(samples)
        if skipMultiAllelicParam is not None:
            transformer.setSkipMultiAllelicParam(skipMultiAllelicParam)
        if skipReference is not None:
            transformer.setSkipReference(skipReference)
        if numPairs is not None:
            transformer.setNumPairs(numPairs)
        return transformer.transform(df)

    def mendel(self, df,  father, mother, child, studyId=None):
        """

        :type df: DataFrame
        """
        transformer = MendelianErrorTransformer().setFather(father).setMother(mother).setChild(child)
        if studyId is not None:
            transformer.setStudyId(studyId)
        return transformer.transform(df)

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
        self.python_utils = PythonUtils()

    def getMetadataPath(self, path):
        return path + ".meta.json.gz"

    def readMetadata(self, meta_path):
        """

        :type meta_path: str
        :param meta_path: Path to the metadata file

        :rtype: dict
        :return: An instance of VariantMetadata
        """
        java_vm = self._call_java("readMetadata", meta_path)
        return self.python_utils.toPythonDict(java_vm)

    def setVariantMetadata(self, df, variant_metadata):
        """

        :type df: DataFrame
        :param df: DataFrame to modify

        :type variant_metadata: VariantMetadata
        :param variant_metadata: VariantMetadata to set

        :rtype: DataFrame
        :return: Modified DataFrame
        """
        java_object = self.python_utils.toJavaObject(variant_metadata, "org.opencb.biodata.models.variant.metadata.VariantMetadata")
        return self._call_java("setVariantMetadata", df, java_object)

    def variantMetadata(self, df):
        java_vm = self._call_java("variantMetadata", df)
        return self.python_utils.toPythonDict(java_vm)

    def samples(self, df, studyId = None):
        if studyId is None:
            return self._call_java("samples", df)
        else:
            return self._call_java("samples", df, studyId)

    def pedigrees(self, df, studyId = None):
        if studyId is None:
            java_vm = self._call_java("pedigrees", df)
            return self.python_utils.toPythonDict(java_vm)
        else:
            java_vm = self._call_java("pedigrees", df, studyId)
            return self.python_utils.toPythonDict(java_vm)


class OskarException(Exception):

    def __init__(self, *args, **kwargs):
        super(OskarException, self).__init__(*args, **kwargs)


class PythonUtils(JavaWrapper):

    def __init__(self):
        super(PythonUtils, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.commons.PythonUtils")

    def toJavaObject(self, python_dict, class_name):
        js = json.dumps(python_dict, ensure_ascii=False)
        return self._call_java("toJavaObject", js, class_name)

    def toPythonDict(self, java_object):
        js = self._call_java("toJsonString", java_object)
        return json.loads(js)
