
import sys
from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer

if sys.version > '3':
    basestring = str

DEFAULT_COHORT = "ALL"


class AbstractTransformer(JavaTransformer, JavaMLReadable, JavaMLWritable):

    def setParams(self, **kwargs):
        filtered = {k: v for k, v in kwargs.items() if v is not None}
        return self._set(**filtered)


class VariantStatsTransformer(AbstractTransformer):
    cohort = Param(Params._dummy(), "cohort", "Name of the cohort to calculate stats from. By default, " + DEFAULT_COHORT,
                   typeConverter=TypeConverters.toString)
    samples = Param(Params._dummy(), "samples", "Samples belonging to the cohort. If empty, will try to read from metadata. "
                    + "If missing, will use all samples from the dataset.", typeConverter=TypeConverters.toListString)
    studyId = Param(Params._dummy(), "studyId", "Id of the study to calculate the stats from.", typeConverter=TypeConverters.toString)
    missingAsReference = Param(Params._dummy(), "missingAsReference", "Count missing alleles as reference alleles.",
                               typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, cohort=None, samples=None, studyId=None, missingAsReference=None):
        super(VariantStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class VariantSetStatsTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    fileId = Param(Params._dummy(), "fileId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, fileId=None):
        super(VariantSetStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getFileId(self):
        return self.getOrDefault(self.fileId)

    def setFileId(self, value):
        return self._set(fileId=value)


class HistogramTransformer(AbstractTransformer):
    step = Param(Params._dummy(), "step", "", typeConverter=TypeConverters.toFloat)
    inputCol = Param(Params._dummy(), "inputCol", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, step=None, inputCol=None):
        super(HistogramTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HistogramTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStep(self):
        return self.getOrDefault(self.step)

    def setStep(self, value):
        return self._set(step=value)

    def getInputCol(self):
        return self.getOrDefault(self.inputCol)

    def setInputCol(self, value):
        return self._set(inputCol=value)


class HardyWeinbergTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None):
        super(HardyWeinbergTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HardyWeinbergTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)


class IBSTransformer(AbstractTransformer):
    samples = Param(Params._dummy(), "samples", "List of samples to use for calculating the IBS",
                    typeConverter=TypeConverters.toListString)
    skipMultiAllelic = Param(Params._dummy(), "skipMultiAllelic", "Skip variants where any of the samples has a secondary alternate",
                             typeConverter=TypeConverters.toBoolean)
    skipReference = Param(Params._dummy(), "skipReference", "Skip variants where both samples of the pair are HOM_REF",
                          typeConverter=TypeConverters.toBoolean)
    numPairs = Param(Params._dummy(), "numPairs", "", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, samples=None, skipMultiAllelic=None, skipReference=None, numPairs=None):
        super(IBSTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.IBSTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class ChiSquareTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, phenotype=None):
        super(ChiSquareTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ChiSquareTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)


class ChromDensityTransformer(AbstractTransformer):
    chroms = Param(Params._dummy(), "chroms", "", typeConverter=TypeConverters.toString)
    step = Param(Params._dummy(), "step", "", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, chroms=None, step=None):
        super(ChromDensityTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ChromDensityTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getChromosome(self):
        return self.getOrDefault(self.chroms)

    def setChromosome(self, value):
        return self._set(chroms=value)

    def getStep(self):
        return self.getOrDefault(self.step)

    def setStep(self, value):
        return self._set(step=value)


class CompoundHeterozigoteTransformer(AbstractTransformer):
    father = Param(Params._dummy(), "father", "", typeConverter=TypeConverters.toString)
    mother = Param(Params._dummy(), "mother", "", typeConverter=TypeConverters.toString)
    child = Param(Params._dummy(), "child", "", typeConverter=TypeConverters.toString)
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    missingGenotypeAsReference = Param(Params._dummy(), "missingGenotypeAsReference", "", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, father=None, mother=None, child=None, studyId=None, missingGenotypeAsReference=None):
        super(CompoundHeterozigoteTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.CompoundHeterozigoteTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class FacetTransformer(AbstractTransformer):
    facet = Param(Params._dummy(), "facet", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, facet=None):
        super(FacetTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.FacetTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getFacet(self):
        return self.getOrDefault(self.facet)

    def setFacet(self, value):
        return self._set(facet=value)


class FisherTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, phenotype=None):
        super(FisherTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.FisherTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)


class ImputeSexTransformer(AbstractTransformer):
    lowerThreshold = Param(Params._dummy(), "lowerThreshold", "", typeConverter=TypeConverters.toFloat)
    upperThreshold = Param(Params._dummy(), "upperThreshold", "", typeConverter=TypeConverters.toFloat)
    chromosomeX = Param(Params._dummy(), "chromosomeX", "", typeConverter=TypeConverters.toString)
    includePseudoautosomalRegions = Param(Params._dummy(), "includePseudoautosomalRegions", "", typeConverter=TypeConverters.toBoolean)
    par1chrX = Param(Params._dummy(), "par1chrX", "", typeConverter=TypeConverters.toString)
    par2chrX = Param(Params._dummy(), "par2chrX", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, lowerThreshold=None, upperThreshold=None, chromosomeX=None, includePseudoautosomalRegions=None, par1chrX=None,
                 par2chrX=None):
        super(ImputeSexTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ImputeSexTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class InbreedingCoefficientTransformer(AbstractTransformer):
    missingGenotypesAsHomRef = Param(Params._dummy(), "missingGenotypesAsHomRef", "Treat missing genotypes as HomRef genotypes",
                                     typeConverter=TypeConverters.toBoolean)
    includeMultiAllelicGenotypes = Param(Params._dummy(), "includeMultiAllelicGenotypes", "Include multi-allelic variants in the calculation",
                                         typeConverter=TypeConverters.toBoolean)
    mafThreshold = Param(Params._dummy(), "mafThreshold", "Include multi-allelic variants in the calculation",
                         typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, missingGenotypesAsHomRef=None, includeMultiAllelicGenotypes=None, mafThreshold=None):
        super(InbreedingCoefficientTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.InbreedingCoefficientTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class MendelianErrorTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    father = Param(Params._dummy(), "father", "", typeConverter=TypeConverters.toString)
    mother = Param(Params._dummy(), "mother", "", typeConverter=TypeConverters.toString)
    child = Param(Params._dummy(), "child", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, father=None, mother=None, child=None):
        super(MendelianErrorTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.MendelianErrorTransformer", self.uid)
        self.setParams(**self._input_kwargs)

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


class ModeOfInheritanceTransformer(AbstractTransformer):
    family = Param(Params._dummy(), "family", "Select family to apply the filter", typeConverter=TypeConverters.toString)
    modeOfInheritance = Param(Params._dummy(), "modeOfInheritance", "Filter by mode of inheritance from a given family. Accepted values: "
                              + "monoallelic (dominant), biallelic (recessive), xLinkedMonoallelic, xLinkedBiallelic, yLinked",
                              typeConverter=TypeConverters.toString)
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)
    incompletePenetrance = Param(Params._dummy(), "incompletePenetrance", "Allow variants with an incomplete penetrance mode of inheritance",
                                 typeConverter=TypeConverters.toBoolean)
    missingAsReference = Param(Params._dummy(), "missingAsReference", "Select family to apply the filter",
                               typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, family=None, modeOfInheritance=None, studyId=None, phenotype=None, incompletePenetrance=None, missingAsReference=None):
        super(ModeOfInheritanceTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.ModeOfInheritanceTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getFamily(self):
        return self.getOrDefault(self.family)

    def setFamily(self, value):
        return self._set(family=value)

    def getModeOfInheritance(self):
        return self.getOrDefault(self.modeOfInheritance)

    def setModeOfInheritance(self, value):
        return self._set(modeOfInheritance=value)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)

    def getIncompletePenetrance(self):
            return self.getOrDefault(self.incompletePenetrance)

    def setIncompletePenetrance(self, value):
        return self._set(incompletePenetrance=value)

    def getMissingAsReference(self):
        return self.getOrDefault(self.missingAsReference)

    def setMissingAsReference(self, value):
        return self._set(missingAsReference=value)


class TdtTransformer(AbstractTransformer):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    phenotype = Param(Params._dummy(), "phenotype", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, phenotype=None):
        super(TdtTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.TdtTransformer", self.uid)
        self.setParams(**self._input_kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getPhenotype(self):
        return self.getOrDefault(self.phenotype)

    def setPhenotype(self, value):
        return self._set(phenotype=value)
