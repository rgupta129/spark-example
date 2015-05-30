# spark-example
Spark Examples using Python and Scala

Setting up and running Python Notebook

ipython profile create pyspark

vim ~/.ipython/profile_pyspark/ipython_notebook_config.py
c = get_config()
c.NotebookApp.port = 9999

vim ~/.ipython/profile_pyspark/startup/00-pyspark-setup.py
import os
import sys

spark_home = os.environ.get('SPARK_HOME', None)
sys.path.insert(0, spark_home + "/python")
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))

export SPARK_HOME=~/Spark/spark
export PYSPARK_SUBMIT_ARGS="--master local[2]"
ipython notebook --profile=pyspark --ip 127.0.0.1

