
import sys

from pyspark.ml.util import _jvm

if sys.version > '3':
    basestring = str

from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql.column import Column, _to_java_column


class VariantUdfManager(JavaWrapper):
    __java_class = None

    @classmethod
    def _java_class(cls):
        if cls.__java_class is None:
            cls.__java_class = _jvm().org.opencb.oskar.spark.variant.udf.VariantUdfManager
        return cls.__java_class

    def __init__(self):
        super(VariantUdfManager, self).__init__()
        self._java_obj = self._new_java_obj("org.opencb.oskar.spark.variant.udf.VariantUdfManager")


    def loadVariantUdfs(self, spark):
        self._call_java("loadVariantUdfs", spark._jsparkSession)


def revcomp(allele):
    jc = VariantUdfManager._java_class().revcomp(_to_java_column(allele))
    return Column(jc)

def include(column, include):
    jc = VariantUdfManager._java_class().include(_to_java_column(column), include)
    return Column(jc)

def includeStudies(column, include):
    jc = VariantUdfManager._java_class().includeStudies(_to_java_column(column), include)
    return Column(jc)

def study(column, studyId):
    jc = VariantUdfManager._java_class().study(_to_java_column(column), studyId)
    return Column(jc)

def file(column, fileId):
    jc = VariantUdfManager._java_class().file(_to_java_column(column), fileId)
    return Column(jc)

def fileAttribute(column, fileId, info):
    jc = VariantUdfManager._java_class().fileAttribute(_to_java_column(column), fileId, info)
    return Column(jc)

def fileFilter(column, fileId):
    jc = VariantUdfManager._java_class().fileFilter(_to_java_column(column), fileId)
    return Column(jc)

def fileQual(column, fileId):
    jc = VariantUdfManager._java_class().fileQual(_to_java_column(column), fileId)
    return Column(jc)

def sampleData(column, sample):
    jc = VariantUdfManager._java_class().sampleData(_to_java_column(column), sample)
    return Column(jc)

def sampleDataField(column, sample):
    jc = VariantUdfManager._java_class().sampleDataField(_to_java_column(column), sample)
    return Column(jc)

def genes(annotationCol):
    jc = VariantUdfManager._java_class().genes(_to_java_column(annotationCol))
    return Column(jc)

def consequenceTypes(annotationCol):
    jc = VariantUdfManager._java_class().consequenceTypes(_to_java_column(annotationCol))
    return Column(jc)

def consequenceTypesByGene(annotationCol, gene):
    jc = VariantUdfManager._java_class().consequenceTypesByGene(_to_java_column(annotationCol), gene)
    return Column(jc)

def proteinSubstitution(annotationCol, score):
    jc = VariantUdfManager._java_class().proteinSubstitution(_to_java_column(annotationCol), score)
    return Column(jc)

def populationFrequency(annotationCol, study, population):
    jc = VariantUdfManager._java_class().populationFrequency(_to_java_column(annotationCol), study, population)
    return Column(jc)

def populationFrequencyAsMap(annotationCol):
    jc = VariantUdfManager._java_class().populationFrequencyAsMap(_to_java_column(annotationCol))
    return Column(jc)

