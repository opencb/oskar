
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

def include_studies(column, include):
    jc = VariantUdfManager._java_class().include_studies(_to_java_column(column), include)
    return Column(jc)

def study(column, studyId):
    jc = VariantUdfManager._java_class().study(_to_java_column(column), studyId)
    return Column(jc)

def file(column, fileId):
    jc = VariantUdfManager._java_class().file(_to_java_column(column), fileId)
    return Column(jc)

def file_attribute(column, fileId, info):
    jc = VariantUdfManager._java_class().file_attribute(_to_java_column(column), fileId, info)
    return Column(jc)

def file_filter(column, fileId):
    jc = VariantUdfManager._java_class().file_filter(_to_java_column(column), fileId)
    return Column(jc)

def file_qual(column, fileId):
    jc = VariantUdfManager._java_class().file_qual(_to_java_column(column), fileId)
    return Column(jc)

def sample_data(column, sample):
    jc = VariantUdfManager._java_class().sample_data(_to_java_column(column), sample)
    return Column(jc)

def sample_data_field(column, sample, formatField):
    jc = VariantUdfManager._java_class().sample_data_field(_to_java_column(column), sample, formatField)
    return Column(jc)

def genes(annotationCol):
    jc = VariantUdfManager._java_class().genes(_to_java_column(annotationCol))
    return Column(jc)

def consequence_types(annotationCol):
    jc = VariantUdfManager._java_class().consequence_types(_to_java_column(annotationCol))
    return Column(jc)

def consequence_types_by_gene(annotationCol, gene):
    jc = VariantUdfManager._java_class().consequence_types_by_gene(_to_java_column(annotationCol), gene)
    return Column(jc)

def protein_substitution(annotationCol, score):
    jc = VariantUdfManager._java_class().protein_substitution(_to_java_column(annotationCol), score)
    return Column(jc)

def population_frequency(annotationCol, study, population):
    jc = VariantUdfManager._java_class().population_frequency(_to_java_column(annotationCol), study, population)
    return Column(jc)

def population_frequency_as_map(annotationCol):
    jc = VariantUdfManager._java_class().population_frequency_as_map(_to_java_column(annotationCol))
    return Column(jc)

