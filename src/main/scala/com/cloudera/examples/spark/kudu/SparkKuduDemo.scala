package com.cloudera.examples.spark.kudu

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark on Kudu Demo
  *
  * Performs insert operations on a Kudu table
  * in Spark.
  *
  */
object SparkKuduDemo {

  // Case class defined with the same schema (column names and types) as the
  // Kudu table we will be writing into
  // Define your case class *outside* your main method
  case class People(id:Int, name:String, gender:String)

  def main(args: Array[String]): Unit = {

    // Setup Spark configuration and related contexts
    val sparkConf = new SparkConf().setAppName("Write Kudu")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Comma-separated list of Kudu masters with port numbers
    val kuduMasters = "quickstart.cloudera:7051"

    // Create an instance of a KuduContext
    val kuduContext = new KuduContext(kuduMasters)

    // This allows us to implicitly convert RDD to DataFrame
    import spark.implicits._

    // Specify a table name
    var kuduTableName = "impala::default.sample_tb"

    val morePeople = Array(People(11, "james", "male"))

    val morePeopleRDD = sc.parallelize(morePeople)

    val morePeopleDF = morePeopleRDD.toDF()

    val kuduOptions: Map[String, String] = Map("kudu.table" -> kuduTableName, "kudu.master" -> kuduMasters)

    println("============================================================================================")
    println("                               DataFrame Before Write ... ")
    println("============================================================================================")
    spark.read.options(kuduOptions).kudu.show

    kuduContext.insertRows(morePeopleDF, kuduTableName)

    println("============================================================================================")
    println("                               DataFrame After Write ... ")
    println("============================================================================================")
    spark.read.options(kuduOptions).kudu.show

    println("============================================================================================")
    println("                               Update DataFrame ... ")
    println("============================================================================================")

    val modifiedPerson = Array(People(11, "james", "female"))

    val modifiedPersonRDD = sc.parallelize(modifiedPerson)

    val modifiedCustomerDF = modifiedPersonRDD.toDF()

    kuduContext.updateRows(modifiedCustomerDF, kuduTableName)

    spark.read.options(kuduOptions).kudu.show

    //
    // DELETE DATA - KUDU CONTEXT
    //
    val df = spark.read.options(kuduOptions).kudu
    df.createOrReplaceTempView("peeps")

    val deleteKeysDF = spark.sql("select id from peeps where id = 1")

    deleteKeysDF.show()

    kuduContext.deleteRows(deleteKeysDF, kuduTableName)

    println("============================================================================================")
    println("                               Delete DataFrame ... ")
    println("============================================================================================")
    spark.read.options(kuduOptions).kudu.show

  }
}
