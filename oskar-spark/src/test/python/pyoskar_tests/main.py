## cd ~/appl/oskar/oskar-spark/src/main/python/ &&  zip -r ~/appl/oskar/oskar.zip oskar ; cd - && ~/appl/spark/bin/pyspark --jars ~/appl/oskar/oskar-spark/target/oskar-spark*-jar-with-dependencies.jar --py-files ~/appl/oskar/oskar.zip


from pyoskar_tests.test_utils import *
from pyoskar.core import Oskar
from pyoskar.spark.analysis import *
from pyoskar.spark.sql import *
from pyoskar.spark.analysis import *
from pyspark.sql.functions import col, udf, count, explode, concat, when, expr
from pyspark.sql.functions import *

spark = create_testing_pyspark_session()

oskar = Oskar(spark)

df = oskar.load(PLATINUM_SMALL) # type: DataFrame

print(oskar.metadata.samples(df))

df.createOrReplaceTempView("chr22")
oskar.metadata.samples(df)
oskar.hardy_weinberg(df)

# Group by type
df.groupBy("type").count().show()
spark.sql("SELECT type, count(*) FROM chr22 GROUP BY type").show()

## Group by variant
# 1)
df.where("type = 'SNV'").select("reference", "alternate").groupBy("reference", "alternate").count().sort("count", ascending=False).show()
# 2)
df.where(col("type").isin("SNV", "SNP")).select("reference", "alternate").groupBy("reference", "alternate").count().sort("count", ascending=False).show()
# 3)
df.where(df.type.isin("SNV", "SNP")).select(df.reference, df.alternate).groupBy(df.reference, df.alternate).count().sort("count", ascending=False).show()
# 4)
spark.sql("SELECT reference, alternate, count(*) FROM chr22 WHERE type = 'SNV' GROUP BY reference,alternate ORDER BY count(*) DESC").show()


# Calculate stats
VariantStatsTransformer().transform(df).select(col("studies")[0].stats).show(truncate=False)
VariantStatsTransformer().transform(df).selectExpr("studies[0].stats.ALL as stats").select("stats.*").show(truncate=False)
oskar.stats(df).selectExpr("studies[0].stats.ALL as stats").select("stats.*").show()
oskar.stats(df, cohort="TRIO", samples=["NA12877", "NA12878", "NA12879"]).selectExpr("studies[0].stats.TRIO as stats").select("stats.*").show()

# Histogram
df_stats = VariantStatsTransformer().transform(df).selectExpr("studies[0].stats.ALL.altAlleleFreq as AF").where("AF >= 0")
HistogramTransformer(step=0.1, inputCol="AF").transform(df_stats).show(10)

# Bucketizer
from pyspark.ml.feature import Bucketizer
import numpy as np
df_stats = VariantStatsTransformer().transform(df).selectExpr("studies[0].stats.ALL.altAlleleFreq as AF").where("AF >= 0")
Bucketizer(inputCol="AF", splits=np.arange(0.0, 1.1, 0.1), outputCol="bucket").transform(df_stats).withColumn("AF", col("bucket") / 10.0).groupBy("AF").count().orderBy("AF").show()




df.select(genes("annotation").alias("genes")).where("genes[0] is not null").show(100)
df.withColumn("genes", genes("annotation")).where("genes[0] is not null").show(100)


df.withColumn("freqs", population_frequency_as_map("annotation")).show(100)
df.withColumn("freqs", population_frequency_as_map("annotation")).where("freqs['1kG_phase3:ALL'] > 0").show(100)


df.select(study("studies", 'hgvauser@platinum:illumina_platinum').alias("platinum")).withColumn("file1", file("platinum", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).where("file1 is not null").show()

df.select(file_attribute("studies", 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'HaplotypeScore')).show(truncate=False)

df.select(study("studies", 'hgvauser@platinum:illumina_platinum').alias("platinum")).select(file("platinum", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).show()
df.select(file(study("studies", 'hgvauser@platinum:illumina_platinum'), "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz").alias("platinum")).where("platinum is not null").show()
df.withColumn("myFile", file("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).where("myFile is not null").show()


df.withColumn("myFile", file(study("studies", 'hgvauser@platinum:illumina_platinum'), "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).where("myFile is not null").show()
df.withColumn("myFile", file(study("studies", 'hgvauser@platinum:illumina_platinum'), "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).withColumn("qual", col("myFile").attributes.QUAL).where("qual < 10").show(100)

df.withColumn("myFile", file(col("studies")[0], "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).where("myFile.attributes.QUAL < 10").show(100)

df.where("file(studies[0], 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz').attributes.QUAL > 10").show(100)
df.selectExpr("file_qual(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz')").show(10)
df.select(file_filter("studies", "platinum-genomes-vcf-NA12877_S1.genome.vcf.gz")).show(truncate=False)

df.select(sample_data("studies", "NA12877").alias("NA12877"), sample_data("studies", "NA12878").alias("NA12878")).show(10)

df.createOrReplaceTempView("chr22")
spark.sql("SELECT id,file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'HaplotypeScore') from chr22 limit 100").show(100)
spark.sql("SELECT id,file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') from chr22 where file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') is not null limit 10").show(100)
spark.sql("SELECT id,file(studies[0], 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz') from chr22 limit 100").show(100)
spark.sql("SELECT id,file(studies[0], 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz').attributes.QUAL from chr22 where file(studies[0], 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz').attributes.QUAL > 100 limit 100").show(100)
spark.sql("SELECT protein_substitution(annotation, 'sift') from chr22").show()
spark.sql("SELECT protein_substitution(annotation, 'sift')[0],protein_substitution(annotation, 'polyphen')[1] from chr22 where protein_substitution(annotation, 'sift')[0] < 0.2").show(100)
# THIS ONES NOT WORKING
# spark.sql("select id,sample(studies, 'NA12877')[0] as NA12877 , sample(studies, 'NA12878')[0] as NA12878 from chr22").show()
# spark.sql("select id,sample(studies, 'NA12877')[0] as NA12877 , sample(studies, 'NA12878')[0] as NA12878 from chr22").where("NA12877=NA12878").groupBy("NA12877").count().sort("count").show(1000)
df.selectExpr("file_filter(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz') AS FILTER").where("array_contains(FILTER, 'LowQD')").show(truncate=False)



import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt


pandas = df.where("type = 'SNV'") \
    .select("reference", "alternate") \
    .groupBy("reference", "alternate") \
    .count() \
    .sort("count", ascending=False) \
    .select(concat(col("reference"), lit(" -> "), col("alternate")).alias("variant"), col("count")) \
    .toPandas()

pandas_norm = df.where("type = 'SNV'") \
    .select(when(expr("reference = 'A' or reference = 'G'"), revcomp(col("reference"))).otherwise(col("reference")).alias("reference"),
            when(expr("reference = 'A' or reference = 'G'"), revcomp(col("alternate"))).otherwise(col("alternate")).alias("alternate")) \
    .groupBy("reference", "alternate") \
    .count() \
    .sort("count", ascending=False) \
    .select(concat(col("reference"), lit(" -> "), col("alternate")).alias("variant"), col("count")) \
    .toPandas()

pandas.plot.bar(x="variant", y="count")
pandas_norm.plot.bar(x="variant", y="count")
plt.show()


# Histogram by QUAL
# df.selectExpr("int(info(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') / 100) * 100  as QUAL").where("QUAL is not null").groupBy("QUAL").count().orderBy("QUAL").show()
qual_df = HistogramTransformer(step=100,inputCol="QUAL").transform(df.selectExpr("file_attribute(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz', 'QUAL') as QUAL").where("QUAL is not null"))
pandas = qual_df.toPandas()
pandas.plot(x='QUAL', y='count')
plt.xlim(0, 4000)
plt.ylim(0)
plt.show()


pandas = oskar.histogram(step=100, inputCol="QUAL", df=df.selectExpr("file_qual(studies, 'platinum-genomes-vcf-NA12877_S1.genome.vcf.gz') as QUAL").where("QUAL is not null")).toPandas()
pandas.plot(x='QUAL', y='count')
plt.xlim(0, 4000)
plt.ylim(0)
plt.show()
