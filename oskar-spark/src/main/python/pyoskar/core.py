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

    def stats(self, df, studyId=None, cohort=None, samples=None, missingAsReference=None):
        """

        :type df: DataFrame
        """
        transformer = VariantStatsTransformer()
        if studyId is not None:
            transformer.setStudyId(studyId)
        if cohort is not None:
            transformer.setCohort(cohort)
        if samples is not None:
            transformer.setSamples(samples)
        if missingAsReference is not None:
            transformer.setMissingAsReference(missingAsReference)
        return transformer.transform(df)

    def globalStats(self, df, studyId=None, fileId=None):
        """

        :type df: DataFrame
        """
        transformer = VariantSetStatsTransformer()
        if studyId is not None:
            transformer.setStudyId(studyId)
        if fileId is not None:
            transformer.setFileId(fileId)
        return transformer.transform(df)

    def histogram(self, df, inputCol, step=None):
        """

        :type df: DataFrame
        """
        transformer = HistogramTransformer().setInputCol(inputCol)
        if step is not None:
            transformer.setStep(step)
        return transformer.transform(df)

    def hardyWeinberg(self, df, studyId):
        """

        :type df: DataFrame
        """
        return HardyWeinbergTransformer().setStudyId(studyId).transform(df)

    def ibs(self, df, samples=None, skipMultiAllelic=None, skipReference=None, numPairs=None):
        """

        :type df: DataFrame
        """
        transformer = IBSTransformer()
        if samples is not None:
            transformer.setSamples(samples)
        if skipMultiAllelic is not None:
            transformer.setSkipMultiAllelic(skipMultiAllelic)
        if skipReference is not None:
            transformer.setSkipReference(skipReference)
        if numPairs is not None:
            transformer.setNumPairs(numPairs)
        return transformer.transform(df)

    def mendel(self, df, father, mother, child, studyId=None):
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

    def chiSquare(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return ChiSquareTransformer().setStudyId(studyId).setPhenotype(phenotype).transform(df)

    def compoundHeterozygote(self, df, father, mother, child, studyId=None, missingGenotypeAsReference=None):
        """

        :type df: DataFrame
        """
        transformer = CompoundHeterozigoteTransformer().setFather(father).setMother(mother).setChild(child)
        if studyId is not None:
            transformer.setStudyId(studyId)
        if missingGenotypeAsReference is not None:
            transformer.setMissingGenotypeAsReference(missingGenotypeAsReference)
        return transformer.transform(df)

    def facet(self, df):
        """

        :type df: DataFrame
        """
        return FacetTransformer().transform(df)

    def fisher(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return FisherTransformer().setStudyId(studyId).setPhenotype(phenotype).transform(df)

    def imputeSex(self, df, lowerThreshold=None, upperThreshold=None, chromosomeX=None, includePseudoautosomalRegions=None, par1chrX=None, par2chrX=None):
        """

        :type df: DataFrame
        """
        transformer = ImputeSexTransformer()
        if lowerThreshold is not None:
            transformer.setLowerThreshold(lowerThreshold)
        if upperThreshold is not None:
            transformer.setUpperThreshold(upperThreshold)
        if chromosomeX is not None:
            transformer.setChromosomeX(chromosomeX)
        if includePseudoautosomalRegions is not None:
            transformer.setIncludePseudoautosomalRegions(includePseudoautosomalRegions)
        if par1chrX is not None:
            transformer.setPar1chrX(par1chrX)
        if par2chrX is not None:
            transformer.setPar2chrX(par2chrX)
        return transformer.transform(df)

    def inbreedingCoefficient(self, df, missingGenotypesAsHomRef=None, includeMultiAllelicGenotypes=None, mafThreshold=None):
        """

        :type df: DataFrame
        """
        transformer = InbreedingCoefficientTransformer()
        if missingGenotypesAsHomRef is not None:
            transformer.setMissingGenotypesAsHomRef(missingGenotypesAsHomRef)
        if includeMultiAllelicGenotypes is not None:
            transformer.setIncludeMultiAllelicGenotypes(includeMultiAllelicGenotypes)
        if mafThreshold is not None:
            transformer.setMafThreshold(mafThreshold)
        return transformer.transform(df)

    def tdt(self, df, studyId, phenotype):
        """

        :type df: DataFrame
        """
        return TdtTransformer().setStudyId(studyId).setPhenotype(phenotype).transform(df)


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
        self._paramMap = {}

    def toJavaObject(self, python_dict, class_name):
        js = json.dumps(python_dict, ensure_ascii=False)
        return self._call_java("toJavaObject", js, class_name)

    def toPythonDict(self, java_object):
        js = self._call_java("toJsonString", java_object)
        return json.loads(js)

    def setKwargs(self, **kwargs):
        """
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None:
                try:
                    value = p.typeConverter(value)
                except TypeError as e:
                    raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self._paramMap[p] = value
        return self
