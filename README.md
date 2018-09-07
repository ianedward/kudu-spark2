# Spark on Kudu - Cloudera Quickstart VM

Spark on Kudu is well integrated and full of great features. Get up and
running quickly with this small sample code project.

This code base demonstrates how to use spark2 with Kudu.

# Building

Modify the Kudu master servers for your environment in the
`SparkKuduDemo.scala` source.

The default is set to cloudera.quickstart

To build this sample code, run:

```sh
mvn clean package
```

# Running samples

The above build does not create an uber jar that will include all the kudu
client side Java and Spark libraries required. The jars will exist in your local
maven repository location right after building, however.

By default that location of these jars is under your `~/.m2` directory. Copy,
symlink, or simply include a direct path to the necessary jars with the `--jars`
option shown in the below `spark-submit` call.

Assuming these are now found in the `lib` directory found in this project,
launch the job with:

```
spark-submit \
--class com.cloudera.examples.spark.kudu.SparkKuduDemo \
--jars kudu-spark_2.2.11-1.2.0.jar spark-kudu-up-and-running-1.0.12.jar
```

