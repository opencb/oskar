
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


def study(studies, studyId):
    jc = VariantUdfManager._java_class().study(_to_java_column(studies), studyId)
    return Column(jc)


def file(studies, fileId):
    jc = VariantUdfManager._java_class().file(_to_java_column(studies), fileId)
    return Column(jc)


def file_attribute(studies, fileId, info):
    jc = VariantUdfManager._java_class().file_attribute(_to_java_column(studies), fileId, info)
    return Column(jc)


def file_filter(studies, fileId):
    jc = VariantUdfManager._java_class().file_filter(_to_java_column(studies), fileId)
    return Column(jc)


def file_qual(studies, fileId):
    jc = VariantUdfManager._java_class().file_qual(_to_java_column(studies), fileId)
    return Column(jc)


def genotype(studies, sample):
    jc = VariantUdfManager._java_class().genotype(_to_java_column(studies), sample)
    return Column(jc)


def sample_data(studies, sample):
    jc = VariantUdfManager._java_class().sample_data(_to_java_column(studies), sample)
    return Column(jc)


def sample_data_field(studies, sample, formatField):
    jc = VariantUdfManager._java_class().sample_data_field(_to_java_column(studies), sample, formatField)
    return Column(jc)


def genes(annotation):
    jc = VariantUdfManager._java_class().genes(_to_java_column(annotation))
    return Column(jc)


def consequence_types(annotation):
    jc = VariantUdfManager._java_class().consequence_types(_to_java_column(annotation))
    return Column(jc)


def consequence_types_by_gene(annotation, gene):
    jc = VariantUdfManager._java_class().consequence_types_by_gene(_to_java_column(annotation), gene)
    return Column(jc)


def protein_substitution(annotation, score):
    jc = VariantUdfManager._java_class().protein_substitution(_to_java_column(annotation), score)
    return Column(jc)


def population_frequency(annotation, study, population):
    jc = VariantUdfManager._java_class().population_frequency(_to_java_column(annotation), study, population)
    return Column(jc)


def population_frequency_as_map(annotation):
    jc = VariantUdfManager._java_class().population_frequency_as_map(_to_java_column(annotation))
    return Column(jc)


def biotypes(annotation):
    jc = VariantUdfManager._java_class().biotypes(_to_java_column(annotation))
    return Column(jc)


def functional(annotation, source):
    jc = VariantUdfManager._java_class().functional(_to_java_column(annotation), source)
    return Column(jc)


def ensembl_genes(annotation):
    jc = VariantUdfManager._java_class().ensembl_genes(_to_java_column(annotation))
    return Column(jc)


def conservation(annotation, source):
    """
    Read the value for the Conservation Score. Null if none. Main conservation scores are: gerp, phastCons and phylop
    :type annotation: str
    :param annotation: annotation field
    :type source: str
    :param source: study source
    :return: conservation score
    """
    jc = VariantUdfManager._java_class().conservation(_to_java_column(annotation), source)
    return Column(jc)
