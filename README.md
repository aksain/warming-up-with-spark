# warming-up-with-spark
Setting up Apache Spark and doing basic programs with its components such as spark-streaming, graphx etc.

# Getting started with Apache Spark
http://www.allprogrammingtutorials.com/tutorials/getting-started-with-apache-spark.php

# Developing Java applications in Apache Spark
http://www.allprogrammingtutorials.com/tutorials/developing-java-applications-in-spark.php

# Getting started with RDDs in Apache Spark
http://www.allprogrammingtutorials.com/tutorials/getting-started-with-rdds-in-spark.php

# Running Spark Cluster Locally
spark-class org.apache.spark.deploy.master.Master --host localhost --port 7077 --webui-port 8080
spark-class org.apache.spark.deploy.worker.Worker  spark://localhost:7077 -c 1 -m 256M
