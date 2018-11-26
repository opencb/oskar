
import sys

if sys.version > '3':
    basestring = str

from pyspark import since, keyword_only, SparkContext
from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.linalg import _convert_to_vector
from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, JavaTransformer, _jvm
from pyspark.ml.common import inherit_doc
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame

DEFAULT_COHORT = "ALL"


class VariantStatsTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    cohort = Param(Params._dummy(), "cohort", "Name of the cohort to calculate stats from. By default, " + DEFAULT_COHORT,
                   typeConverter=TypeConverters.toString)
    samples = Param(Params._dummy(), "samples", "Samples belonging to the cohort. If empty, will try to read from metadata. "
                    + "If missing, will use all samples from the dataset.", typeConverter=TypeConverters.toListString)
    studyId = Param(Params._dummy(), "studyId", "Id of the study to calculate the stats from.", typeConverter=TypeConverters.toString)
    missingAsReference = Param(Params._dummy(), "missingAsReference", "Count missing alleles as reference alleles.",
                               typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self):
        super(VariantStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getCohort(self):
        return self.getOrDefault(self.cohort)

    def setCohort(self, value):
        return self._set(cohort=value)

    def getSamples(self):
        return self.getOrDefault(self.samples)

    def setSamples(self, value):
        return self._set(samples=value)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getMissingAsReference(self):
        return self.getOrDefault(self.missingAsReference)

    def setMissingAsReference(self, value):
        return self._set(missingAsReference=value)

    def transformDataframe(self, df):
        return self._call_java(df)


class VariantSetStatsTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    fileId = Param(Params._dummy(), "fileId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(VariantSetStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getFileId(self):
        return self.getOrDefault(self.fileId)

    def setFileId(self, value):
        return self._set(fileId=value)


class HistogramTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    step = Param(Params._dummy(), "step", "", typeConverter=TypeConverters.toFloat)
    inputCol = Param(Params._dummy(), "inputCol", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        """

        :type inputCol: str
        :type step: float
        """
        super(HistogramTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HistogramTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStep(self):
        return self.getOrDefault(self.step)

    def setStep(self, value):
        return self._set(step=value)

    def getInputCol(self):
        return self.getOrDefault(self.inputCol)

    def setInputCol(self, value):
        return self._set(inputCol=value)


class HardyWeinbergTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(HardyWeinbergTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HardyWeinbergTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)


class IBSTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    samples = Param(Params._dummy(), "samples", "List of samples to use for calculating the IBS",
                    typeConverter=TypeConverters.toListString)
    skipMultiAllelic = Param(Params._dummy(), "skipMultiAllelic", "Skip variants where any of the samples has a secondary alternate",
                             typeConverter=TypeConverters.toBoolean)
    skipReference = Param(Params._dummy(), "skipReference", "Skip variants where both samples of the pair are HOM_REF",
                          typeConverter=TypeConverters.toBoolean)
    numPairs = Param(Params._dummy(), "numPairs", "", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self):
        super(IBSTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.IBSTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getSamples(self):
        return self.getOrDefault(self.samples)

    def setSamples(self, value):
        return self._set(samples=value)

    def getSkipMultiAllelic(self):
        return self.getOrDefault(self.skipMultiAllelic)

    def setSkipMultiAllelic(self, value):
        return self._set(skipMultiAllelic=value)

    def getSkipReference(self):
        return self.getOrDefault(self.skipReference)

    def setSkipReference(self, value):
        return self._set(skipReference=value)

    def getNumPairs(self):
        return self.getOrDefault(self.numPairs)

    def setNumPairs(self, value):
        return self._set(numPairs=value)


class MendelianErrorTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    father = Param(Params._dummy(), "father", "", typeConverter=TypeConverters.toString)
    mother = Param(Params._dummy(), "mother", "", typeConverter=TypeConverters.toString)
    child = Param(Params._dummy(), "child", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(MendelianErrorTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.MendelianErrorTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getFather(self):
        return self.getOrDefault(self.father)

    def setFather(self, value):
        return self._set(father=value)

    def getMother(self):
        return self.getOrDefault(self.mother)

    def setMother(self, value):
        return self._set(mother=value)

    def getChild(self):
        return self.getOrDefault(self.child)

    def setChild(self, value):
        return self._set(child=value)


class ChiSquareTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(ChiSquareTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ChiSquareTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)


class CompoundHeterozigoteTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    father = Param(Params._dummy(), "father", "", typeConverter=TypeConverters.toString)
    mother = Param(Params._dummy(), "mother", "", typeConverter=TypeConverters.toString)
    child = Param(Params._dummy(), "child", "", typeConverter=TypeConverters.toString)
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    missingGenotypeAsReference = Param(Params._dummy(), "missingGenotypeAsReference", "", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self):
        super(CompoundHeterozigoteTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.CompoundHeterozigoteTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getFather(self):
        return self.getOrDefault(self.father)

    def setFather(self, value):
        return self._set(father=value)

    def getMother(self):
        return self.getOrDefault(self.mother)

    def setMother(self, value):
        return self._set(mother=value)

    def getChild(self):
        return self.getOrDefault(self.child)

    def setChild(self, value):
        return self._set(child=value)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getMissingGenotypeAsReference(self):
        return self.getOrDefault(self.missingGenotypeAsReference)

    def setMissingGenotypeAsReference(self, value):
        return self._set(missingGenotypeAsReference=value)


class FacetTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self):
        super(FacetTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.FacetTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)


class FisherTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(FisherTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.FisherTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)


class ImputeSexTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    lowerThreshold = Param(Params._dummy(), "lowerThreshold", "", typeConverter=TypeConverters.toFloat)
    upperThreshold = Param(Params._dummy(), "upperThreshold", "", typeConverter=TypeConverters.toFloat)
    chromosomeX = Param(Params._dummy(), "chromosomeX", "", typeConverter=TypeConverters.toString)
    includePseudoautosomalRegions = Param(Params._dummy(), "includePseudoautosomalRegions", "", typeConverter=TypeConverters.toBoolean)
    par1chrX = Param(Params._dummy(), "par1chrX", "", typeConverter=TypeConverters.toString)
    par2chrX = Param(Params._dummy(), "par2chrX", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(ImputeSexTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ImputeSexTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getLowerThreshold(self):
        return self.getOrDefault(self.lowerThreshold)

    def setLowerThreshold(self, value):
        return self._set(lowerThreshold=value)

    def getUpperThreshold(self):
        return self.getOrDefault(self.upperThreshold)

    def setUpperThreshold(self, value):
        return self._set(upperThreshold=value)

    def getChromosomeX(self):
        return self.getOrDefault(self.chromosomeX)

    def setChromosomeX(self, value):
        return self._set(chromosomeX=value)

    def getIncludePseudoautosomalRegions(self):
        return self.getOrDefault(self.includePseudoautosomalRegions)

    def setIncludePseudoautosomalRegions(self, value):
        return self._set(includePseudoautosomalRegions=value)

    def getPar1chrX(self):
        return self.getOrDefault(self.par1chrX)

    def setPar1chrX(self, value):
        return self._set(par1chrX=value)

    def getPar2chrX(self):
        return self.getOrDefault(self.par2chrX)

    def setPar2chrX(self, value):
        return self._set(par2chrX=value)


class InbreedingCoefficientTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    missingGenotypesAsHomRef = Param(Params._dummy(), "missingGenotypesAsHomRef", "Treat missing genotypes as HomRef genotypes",
                                     typeConverter=TypeConverters.toBoolean)
    includeMultiAllelicGenotypes = Param(Params._dummy(), "includeMultiAllelicGenotypes", "Include multi-allelic variants in the calculation",
                                         typeConverter=TypeConverters.toBoolean)
    mafThreshold = Param(Params._dummy(), "mafThreshold", "Include multi-allelic variants in the calculation",
                         typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self):
        super(InbreedingCoefficientTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.InbreedingCoefficientTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getMissingGenotypesAsHomRef(self):
        return self.getOrDefault(self.missingGenotypesAsHomRef)

    def setMissingGenotypesAsHomRef(self, value):
        return self._set(missingGenotypesAsHomRef=value)

    def getIncludeMultiAllelicGenotypes(self):
        return self.getOrDefault(self.includeMultiAllelicGenotypes)

    def setIncludeMultiAllelicGenotypes(self, value):
        return self._set(includeMultiAllelicGenotypes=value)

    def getMafThreshold(self):
        return self.getOrDefault(self.mafThreshold)

    def setMafThreshold(self, value):
        return self._set(mafThreshold=value)


class TdtTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(TdtTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.TdtTransformer", self.uid)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)
