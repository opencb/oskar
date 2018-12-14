#!/bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'
export SPARK_CONF_DIR='../conf'

if [[ -z "$SPARK_HOME" ]]; then
  '~/soft/spark-2.4.0-bin-hadoop2.7/bin/pyspark'
else
  $SPARK_HOME/bin/pyspark
fi
