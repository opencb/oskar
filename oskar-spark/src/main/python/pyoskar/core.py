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
        self.python_utils = PythonUtils()

    def load(self, file_path):
        """
        :type file_path: str
        """
        df = self._call_java("load", file_path)
        return df

    def chiSquare(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return ChiSquareTransformer(studyId=studyId, phenotype=phenotype).transform(df)

    def compoundHeterozygote(self, df, father, mother, child, studyId=None, missingGenotypeAsReference=None):
        """

        :type df: DataFrame
        """
        return CompoundHeterozigoteTransformer(father=father, mother=mother, child=child, studyId=studyId,
                                               missingGenotypeAsReference=missingGenotypeAsReference).transform(df)

    def facet(self, df, facet):
        """

        :type df: DataFrame
        """
        return FacetTransformer(facet=facet).transform(df)

    def fisher(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return FisherTransformer(studyId=studyId, phenotype=phenotype).transform(df)

    def hardyWeinberg(self, df, studyId=None):
        """

        :type df: DataFrame
        """
        return HardyWeinbergTransformer(studyId=studyId).transform(df)

    def histogram(self, df, inputCol, step=None):
        """

        :type df: DataFrame
        """
        return HistogramTransformer(inputCol=inputCol, step=step).transform(df)

    def ibs(self, df, samples=None, skipMultiAllelic=None, skipReference=None, numPairs=None):
        """

        :type df: DataFrame
        """
        return IBSTransformer(samples=samples, skipReference=skipReference, skipMultiAllelic=skipMultiAllelic,
                              numPairs=numPairs).transform(df)

    def imputeSex(self, df, lowerThreshold=None, upperThreshold=None, chromosomeX=None, includePseudoautosomalRegions=None, par1chrX=None, par2chrX=None):
        """

        :type df: DataFrame
        """
        return ImputeSexTransformer(lowerThreshold=lowerThreshold, upperThreshold=upperThreshold, chromosomeX=chromosomeX,
                                    includePseudoautosomalRegions=includePseudoautosomalRegions, par1chrX=par1chrX, par2chrX=par2chrX).transform(df)

    def inbreedingCoefficient(self, df, missingGenotypesAsHomRef=None, includeMultiAllelicGenotypes=None, mafThreshold=None):
        """

        :type df: DataFrame
        """
        return InbreedingCoefficientTransformer(missingGenotypesAsHomRef=missingGenotypesAsHomRef,
                                                includeMultiAllelicGenotypes=includeMultiAllelicGenotypes, mafThreshold=mafThreshold).transform(df)

    def mendel(self, df, father, mother, child, studyId=None):
        """

        :type df: DataFrame
        """
        return MendelianErrorTransformer(father=father, mother=mother, child=child, studyId=studyId).transform(df)

    # def de_novo(self, df):
    # def ld_matrix(self, df):
    # def impute_sex(self, df):
    # def hwe_normalized_pca(self, df):
    # def concordance(self, df):
    # def cancer_signature(self, df): #https://cancer.sanger.ac.uk/cosmic/signatures

    def modeOfInheritance(self, df, family, modeOfInheritance, phenotype, studyId=None, incompletePenetrance=None, missingAsReference=None):
        """

        :type df: DataFrame
        """
        return ModeOfInheritanceTransformer(family=family, modeOfInheritance=modeOfInheritance, phenotype=phenotype, studyId=studyId,
                                            incompletePenetrance=incompletePenetrance, missingAsReference=missingAsReference).transform(df)

    def tdt(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return TdtTransformer(studyId=studyId, phenotype=phenotype).transform(df)

    def stats(self, df, studyId=None, cohort=None, samples=None, missingAsReference=None):
        """

        :type df: DataFrame
        """
        return VariantStatsTransformer(studyId=studyId, cohort=cohort, samples=samples, missingAsReference=missingAsReference).transform(df)

    def globalStats(self, df, studyId=None, fileId=None):
        """

        :type df: DataFrame
        """
        return VariantSetStatsTransformer(studyId=studyId, fileId=fileId).transform(df)


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
        java_object = self.python_utils.toJavaObject(variant_metadata,
                                                     "org.opencb.biodata.models.variant.metadata.VariantMetadata")
        return self._call_java("setVariantMetadata", df, java_object)

    def variantMetadata(self, df):
        java_vm = self._call_java("variantMetadata", df)
        return self.python_utils.toPythonDict(java_vm)

    def samples(self, df, studyId=None):
        if studyId is None:
            return self._call_java("samples", df)
        else:
            return self._call_java("samples", df, studyId)

    def pedigrees(self, df, studyId=None):
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
