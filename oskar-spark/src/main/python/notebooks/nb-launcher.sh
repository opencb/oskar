#!/bin/bash

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
export SPARK_CONF_DIR='../conf'

if [[ -z "$SPARK_HOME" ]]; then
  ~/spark-2.3.2-bin-hadoop2.7/bin/pyspark
else
  $SPARK_HOME/bin/pyspark
fi
