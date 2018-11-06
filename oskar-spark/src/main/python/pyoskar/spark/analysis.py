
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


class VariantStatsTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    cohort = Param(Params._dummy(), "cohort", "", typeConverter=TypeConverters.toString)
    samples = Param(Params._dummy(), "samples", "", typeConverter=TypeConverters.toListString)
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, cohort="ALL", samples=None):
        super(VariantStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantStatsTransformer", self.uid)
        self._setDefault(cohort="ALL")
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
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


class VariantSetStatsTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)
    fileId = Param(Params._dummy(), "fileId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None, fileId=None):
        super(VariantSetStatsTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.VariantSetStatsTransformer", self.uid)
        self._setDefault(studyId="")
        self._setDefault(fileId="")
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

    def getFileId(self):
        return self.getOrDefault(self.fileId)

    def setFileId(self, value):
        return self._set(fileId=value)


class MendelianErrorTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self):
        super(MendelianErrorTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.MendelianErrorTransformer", self.uid)
        # self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Binarizer", self.uid)
        # self._setDefault(studyId="")
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
        self._set(**kwargs)

    def getStudyId(self):
        return self.getOrDefault(self.studyId)

    def setStudyId(self, value):
        return self._set(studyId=value)

class IBSTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    samples = Param(Params._dummy(), "samples", "", typeConverter=TypeConverters.toListInt)
    skipReference = Param(Params._dummy(), "skipReference", "", typeConverter=TypeConverters.toBoolean)
    numPairs = Param(Params._dummy(), "numPairs", "", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, skipReference=False, samples=None, numPairs=None):
        super(IBSTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.IBSTransformer", self.uid)
        self._setDefault(skipReference=False)
        # self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Binarizer", self.uid)
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
        self._set(**kwargs)

    def setSamples(self, value):
        return self._set(samples=value)

    def setSkipReference(self, value):
        return self._set(skipReference=value)

    def setNumPairs(self, value):
        return self._set(numPairs=value)


class HistogramTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    step = Param(Params._dummy(), "step", "", typeConverter=TypeConverters.toFloat)
    inputCol = Param(Params._dummy(), "inputCol", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, step=None, inputCol=None):
        """

        :type inputCol: str
        :type step: float
        """
        super(HistogramTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HistogramTransformer", self.uid)
        self._setDefault(step=0.1)
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
        self._set(**kwargs)

    # @keyword_only
    # @since("1.4.0")
    # def setParams(self, step=None, inputCol=None):
    #     kwargs = self._input_kwargs
    #     return self._set(**kwargs)


class HardyWeinbergTransformer(JavaTransformer, HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    studyId = Param(Params._dummy(), "studyId", "", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, studyId=None):
        super(HardyWeinbergTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.analysis.HardyWeinbergTransformer", self.uid)
        kwargs = self._input_kwargs
        # self.setParams(**kwargs)
        self._set(**kwargs)