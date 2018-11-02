
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
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructType, MapType
from pyspark.sql.column import Column, _to_java_column, _to_seq


def loadVariantUdfs(spark):
    import json

    sqlContext = spark.udf.sqlContext

    sqlContext.registerJavaFunction("revcomp", "org.opencb.oskar.spark.variant.udf.RevcompFunction",
                                    StringType())

    json = json.loads(VariantUdfManager().getStudyDataType())
    studyDataType = StructType.fromJson(json)
    fileDataType = studyDataType["files"].dataType.elementType

    sqlContext.registerJavaFunction("includeStudy", "org.opencb.oskar.spark.variant.udf.IncludeStudyFunction",
                                    ArrayType(studyDataType))
    sqlContext.registerJavaFunction("study", "org.opencb.oskar.spark.variant.udf.StudyFunction",
                                    studyDataType)
    sqlContext.registerJavaFunction("file", "org.opencb.oskar.spark.variant.udf.FileFunction",
                                    fileDataType)
    sqlContext.registerJavaFunction("info", "org.opencb.oskar.spark.variant.udf.FileAttributeFunction",
                                    StringType())
    sqlContext.registerJavaFunction("filter", "org.opencb.oskar.spark.variant.udf.FileFilterFunction",
                                    ArrayType(StringType()))
    sqlContext.registerJavaFunction("qual", "org.opencb.oskar.spark.variant.udf.FileQualFunction",
                                    DoubleType())
    sqlContext.registerJavaFunction("sampleData", "org.opencb.oskar.spark.variant.udf.SampleDataFunction",
                                    ArrayType(StringType()))
    sqlContext.registerJavaFunction("sampleDataField", "org.opencb.oskar.spark.variant.udf.SampleDataFieldFunction",
                                    ArrayType(StringType()))

    sqlContext.registerJavaFunction("genes", "org.opencb.oskar.spark.variant.udf.GenesFunction",
                                    ArrayType(StringType()))
    sqlContext.registerJavaFunction("consequenceType", "org.opencb.oskar.spark.variant.udf.ConsequenceTypesFunction",
                                    ArrayType(StringType()))
    sqlContext.registerJavaFunction("consequenceTypeByGene", "org.opencb.oskar.spark.variant.udf.ConsequenceTypesByGeneFunction",
                                    ArrayType(StringType()))
    sqlContext.registerJavaFunction("proteinSubstitution", "org.opencb.oskar.spark.variant.udf.ProteinSubstitutionScoreFunction",
                                    ArrayType(DoubleType()))
    sqlContext.registerJavaFunction("populationFrequency", "org.opencb.oskar.spark.variant.udf.PopFreqFunction",
                                    DoubleType())
    sqlContext.registerJavaFunction("populationFrequencyAsMap", "org.opencb.oskar.spark.variant.udf.PopFreqAsMapFunction",
                                    MapType(StringType(), DoubleType()))



class VariantUdfManager(JavaWrapper):
    def __init__(self):
        super(VariantUdfManager, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.udf.VariantUdfManager")

    def helloWorld(self):
        self._call_java("helloWorld")

    def getStudyDataType(self):
        return self._call_java("getStudyDataType")


def revcomp(allele):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.revcomp(_to_java_column(allele))
    return Column(jc)

def include(column, include):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.include(_to_java_column(column), include)
    return Column(jc)

def includeStudies(column, include):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.includeStudies(_to_java_column(column), include)
    return Column(jc)

def study(column, studyId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.study(_to_java_column(column), studyId)
    return Column(jc)

def file(column, fileId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.file(_to_java_column(column), fileId)
    return Column(jc)

def info(column, fileId, info):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.info(_to_java_column(column), fileId, info)
    return Column(jc)

def filter(column, fileId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.filter(_to_java_column(column), fileId)
    return Column(jc)

def qual(column, fileId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.qual(_to_java_column(column), fileId)
    return Column(jc)

def sample(column, sample):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.sample(_to_java_column(column), sample)
    return Column(jc)

def genes(annotationCol):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.genes(_to_java_column(annotationCol))
    return Column(jc)

def consequenceType(annotationCol):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.consequenceType(_to_java_column(annotationCol))
    return Column(jc)

def consequenceTypeByGene(annotationCol, gene=""):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.consequenceTypeByGene(_to_java_column(annotationCol), gene)
    return Column(jc)

def proteinSubstitution(annotationCol, score):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.proteinSubstitution(_to_java_column(annotationCol), score)
    return Column(jc)

def populationFrequency(annotationCol, study, population):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.populationFrequency(_to_java_column(annotationCol), study, population)
    return Column(jc)

def populationFrequencyAsMap(annotationCol):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.populationFrequencyAsMap(_to_java_column(annotationCol))
    return Column(jc)

