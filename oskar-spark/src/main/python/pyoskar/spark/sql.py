
import sys

if sys.version > '3':
    basestring = str

from pyspark import SparkContext
from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql.column import Column, _to_java_column


class VariantUdfManager(JavaWrapper):
    def __init__(self):
        super(VariantUdfManager, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.udf.VariantUdfManager")


    def loadVariantUdfs(self, spark):
        sqlContext = spark.udf.sqlContext

        udfs = self.getUdfs()
        for udf in udfs:
            clazz = self.getUdfClassName(udf)
            data_type = self.getUdfReturnType(udf)
            sqlContext.registerJavaFunction(udf, clazz, data_type)

    def getUdfs(self):
        return self._call_java("getUdfs")

    def getUdfClassName(self, udf):
        return self._call_java("getUdfClassName", udf)

    def getUdfReturnTypeAsJson(self, udf):
        return self._call_java("getUdfReturnTypeAsJson", udf)

    def getUdfReturnType(self, udf):
        from pyspark.sql.types import _parse_datatype_json_string

        jsonStr = self.getUdfReturnTypeAsJson(udf)
        return _parse_datatype_json_string(jsonStr)

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

def fileAttribute(column, fileId, info):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.fileAttribute(_to_java_column(column), fileId, info)
    return Column(jc)

def fileFilter(column, fileId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.fileFilter(_to_java_column(column), fileId)
    return Column(jc)

def fileQual(column, fileId):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.fileQual(_to_java_column(column), fileId)
    return Column(jc)

def sampleData(column, sample):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.sampleData(_to_java_column(column), sample)
    return Column(jc)

def sampleDataField(column, sample):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.sampleDataField(_to_java_column(column), sample)
    return Column(jc)

def genes(annotationCol):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.genes(_to_java_column(annotationCol))
    return Column(jc)

def consequenceTypes(annotationCol):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.consequenceTypes(_to_java_column(annotationCol))
    return Column(jc)

def consequenceTypesByGene(annotationCol, gene=""):
    sc = SparkContext._active_spark_context
    jc = sc._jvm.org.opencb.oskar.spark.variant.udf.VariantUdfManager.consequenceTypesByGene(_to_java_column(annotationCol), gene)
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

