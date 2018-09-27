import os

os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['SPARK_LOG_DIR'] = '/var/log/spark'
os.environ['HADOOP_HOME'] = '/usr/lib/hadoop'
os.environ['PYSPARK_SUBMIT_ARGS'] = '"--name" "PySparkShell" "pyspark-shell"'
os.environ['PYTHONSTARTUP'] = '/usr/lib/spark/python/pyspark.shell.py'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['SPARK_ENV_LOADED'] = '1'
os.environ['AWS_PATH'] = '/opt/aws'
os.environ['AWS_AUTO_SCALING_HOME'] = '/opt/aws/apitools/as'
os.environ['SPARK_DAEMON_JAVA_OPTS'] = ' -XX:OnOutOfMemoryError=\'kill -9 %p\''
os.environ['SPARK_WORKER_DIR'] = '/var/run/spark/work'
os.environ['SPARK_SCALA_VERSION'] = '2.10'

import sys
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.4-src.zip')
sys.path.append('/usr/lib/spark/python/')

import pyspark
from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.setMaster('yarn-client')
