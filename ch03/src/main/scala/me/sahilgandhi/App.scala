package me.sahilgandhi

/**
 * Hello world!
 *
 */

import org.apache.spark.sql.SparkSession

object App extends Application {
  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/DATA/github-archive/2015-01-01-0.json"
    val ghLog = spark.read.json(inputPath)
    

  }
}
