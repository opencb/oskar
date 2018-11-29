import json

from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql.dataframe import DataFrame

from pyoskar.analysis import *

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

        :param file_path:
        :return:
        """
        df = self._call_java("load", file_path)
        return df

    def chiSquare(self, df, studyId, phenotype):
        """

        :param df:
        :param studyId:
        :param phenotype:
        :return:
        """
        return ChiSquareTransformer(studyId=studyId, phenotype=phenotype).transform(df)

    def compoundHeterozygote(self, df, father, mother, child, studyId=None, missingGenotypeAsReference=None):
        """

        :param df:
        :param father:
        :param mother:
        :param child:
        :param studyId:
        :param missingGenotypeAsReference:
        :return:
        """
        return CompoundHeterozigoteTransformer(father=father, mother=mother, child=child, studyId=studyId,
                                               missingGenotypeAsReference=missingGenotypeAsReference).transform(df)

    def facet(self, df, facet):
        """

        :param df:
        :param facet:
        :return:
        """
        return FacetTransformer(facet=facet).transform(df)

    def fisher(self, df, studyId, phenotype):
        """

        :param df:
        :param studyId:
        :param phenotype:
        :return:
        """
        return FisherTransformer(studyId=studyId, phenotype=phenotype).transform(df)

    def hardyWeinberg(self, df, studyId=None):
        """

        :type df: DataFrame
        :param df: Original dataframe

        :type studyId: str
        :param studyId:

        :rtype: DataFrame
        :return: Transformed dataframe
        """
        return HardyWeinbergTransformer(studyId=studyId).transform(df)

    def histogram(self, df, inputCol, step=None):
        """

        :param df:
        :param inputCol:
        :param step:
        :return:
        """
        return HistogramTransformer(inputCol=inputCol, step=step).transform(df)

    def ibs(self, df, samples=None, skipMultiAllelic=None, skipReference=None, numPairs=None):
        """
        Calculates the Identity By State.

        :type df: DataFrame
        :param df: Original dataframe

        :type samples: list<str>
        :param samples: List of samples to use for calculating the IBS

        :type skipMultiAllelic: bool
        :param skipMultiAllelic: Skip variants where any of the samples has a secondary alternate

        :type skipReference: bool
        :param skipReference: Skip variants where both samples of the pair are HOM_REF

        :type numPairs: int
        :param numPairs:

        :rtype: DataFrame
        :return: Transformed dataframe
        """
        return IBSTransformer(samples=samples, skipReference=skipReference, skipMultiAllelic=skipMultiAllelic,
                              numPairs=numPairs).transform(df)

    def imputeSex(self, df, lowerThreshold=None, upperThreshold=None, chromosomeX=None, includePseudoautosomalRegions=None, par1chrX=None, par2chrX=None):
        """
        Estimate sex of the individuals calculating the inbreeding coefficients F on the chromosome X.

        :type df: DataFrame
        :param df: Original dataframe

        :type lowerThreshold: float
        :param lowerThreshold:

        :type upperThreshold: float
        :param upperThreshold:

        :type chromosomeX: str
        :param chromosomeX:

        :type includePseudoautosomalRegions: bool
        :param includePseudoautosomalRegions:

        :type par1chrX: str
        :param par1chrX:

        :type par2chrX: str
        :param par2chrX:

        :rtype: DataFrame
        :return: Transformed dataframe
        """
        return ImputeSexTransformer(lowerThreshold=lowerThreshold, upperThreshold=upperThreshold, chromosomeX=chromosomeX,
                                    includePseudoautosomalRegions=includePseudoautosomalRegions, par1chrX=par1chrX, par2chrX=par2chrX).transform(df)

    def inbreedingCoefficient(self, df, missingGenotypesAsHomRef=None, includeMultiAllelicGenotypes=None, mafThreshold=None):
        """
        Count observed and expected autosomal homozygous genotype for each sample, and report method-of-moments F coefficient estimates. (Ritland, Kermit. 1996)
        Values:
         - Total genotypes Count : Total count of genotypes for sample
         - Observed homozygotes  : Count of observed homozygote genotypes for each sample, in each variant
         - Expected homozygotes  : Count of expected homozygote genotypes for each sample, in each variant.
                 Calculated with the MAF of the cohort ALL. 1.0−(2.0∗maf∗(1.0−maf))
         - F                     : Inbreeding coefficient. Calculated as:
                 ([observed hom. count] - [expected count]) / ([total genotypes count] - [expected count])
        Unless otherwise specified, the genotype counts will exclude the missing and multi-allelic genotypes.

        :type df: DataFrame
        :param df: Original dataframe

        :type missingGenotypesAsHomRef: bool
        :param missingGenotypesAsHomRef: Treat missing genotypes as HomRef genotypes

        :type includeMultiAllelicGenotypes: bool
        :param includeMultiAllelicGenotypes: Include multi-allelic variants in the calculation

        :type mafThreshold: float
        :param mafThreshold: Include multi-allelic variants in the calculation

        :rtype: DataFrame
        :return: Transformed dataframe
        """
        return InbreedingCoefficientTransformer(missingGenotypesAsHomRef=missingGenotypesAsHomRef,
                                                includeMultiAllelicGenotypes=includeMultiAllelicGenotypes, mafThreshold=mafThreshold).transform(df)

    def mendel(self, df, father, mother, child, studyId=None):
        """
        Using Plink Mendel error codes
        https://www.cog-genomics.org/plink2/basic_stats#mendel

        :type df: DataFrame
        :param df: Original dataframe

        :type father: str
        :param father:

        :type mother: str
        :param mother:

        :type child: str
        :param child:

        :type studyId: str
        :param studyId:


        :rtype: DataFrame
        :return: Transformed dataframe
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
        Filter variants that match a given Mode Of Inheritance pattern.

        Accepted patterns:
         - monoallelic, also known as dominant
         - biallelic, also known as recessive
         - xLinked
         - yLinked

        :type df: DataFrame
        :param df: Original dataframe

        :type family: str
        :param family: Select family to apply the filter

        :type modeOfInheritance: str
        :param modeOfInheritance: Filter by mode of inheritance from a given family. Accepted values: monoallelic (dominant),
                                  biallelic (recessive), xLinkedMonoallelic, xLinkedBiallelic, yLinked"

        :type phenotype: str
        :param phenotype:

        :type studyId: str
        :param studyId:

        :type incompletePenetrance: bool
        :param incompletePenetrance: Allow variants with an incomplete penetrance mode of inheritance

        :type missingAsReference: bool
        :param missingAsReference:

        :rtype: DataFrame
        :return: Transformed dataframe
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
        :param df: Original dataframe

        :type studyId: str
        :param studyId:

        :type cohort: str
        :param cohort: Name of the cohort to calculate stats from. By default, 'ALL'

        :type samples: list<str>
        :param samples: Samples belonging to the cohort. If empty, will try to read from metadata. If missing, will use all samples
                        from the dataset

        :type missingAsReference: bool
        :param missingAsReference: Count missing alleles as reference alleles

        :rtype: DataFrame
        :return: Transformed dataframe
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
        Writes the VariantMetadata into the schema metadata from the given dataframe.

        :type meta_path: str
        :param meta_path: Path to the metadata file

        :rtype: dict
        :return: An instance of VariantMetadata
        """
        java_vm = self._call_java("readMetadata", meta_path)
        return self.python_utils.toPythonDict(java_vm)

    def setVariantMetadata(self, df, variant_metadata):
        """
        Writes the VariantMetadata into the schema metadata from the given dataframe.

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
