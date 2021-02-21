package com.trendyol.bootcamp.homework

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._


object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Dataset Examples")
      .getOrCreate()

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val initial_data= spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("data/homework/initial_data.json").as[Product]

    val cdc_data= spark.read
      .format("json")
      .option("inferSchema","true")
      .load("data/homework/cdc_data.json").as[Product]

    val cdc_data_ordered= cdc_data.orderBy(col("id").asc, col("timestamp").desc).as[Product]
    val cdc_data_last= cdc_data_ordered.groupBy("id").agg(max("timestamp")
      .alias("last_timestamp")).withColumnRenamed("id","last_id")

    val joinExpression = cdc_data_ordered.col("id") === cdc_data_last.col("last_id")
    val joinType= "left_outer"

    // DataFrame Solution

    /*
    val cdc_data_filtered= cdc_data_ordered.join(cdc_data_last,joinExpression,joinType)
      .filter(col("timestamp") === col("last_timestamp"))
      .drop("last_id").drop("last_timestamp").as[Product].show()
     */

    // DataSet Solution

    def data_filter(row : ProductJoin) : Boolean={
      row.timestamp == row.last_timestamp
    }

    val cdc_data_filtered= cdc_data_ordered.join(cdc_data_last,joinExpression,joinType).as[ProductJoin]
      .filter(row=>data_filter(row)).drop("last_id").drop("last_timestamp").as[Product]


    // Update initial_data

    val all_data= initial_data.union(cdc_data_filtered).orderBy(col("id").asc).as[Product]
    val all_data_last= all_data.groupBy("id").agg(max("timestamp")
      .alias("last_timestamp")).withColumnRenamed("id","last_id")

    val joinExpression_update = all_data.col("id") === all_data_last.col("last_id")
    val joinType_update= "left_outer"

    val snapshot_data= all_data.join(all_data_last,joinExpression_update,joinType_update).as[ProductJoin]
      .filter(row=>data_filter(row)).drop("last_id").drop("last_timestamp").as[Product]


    // Write updated data to snapshot_data

    snapshot_data
      .coalesce(1)
      .write
      .format("json")
      .mode("overwrite")
      .save("data/homework/snapshot_data")

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

  }

}

case class Product(
                  brand: String,
                  category: String,
                  color: String,
                  id: Long,
                  name: String,
                  price: Double,
                  timestamp: Long)

case class ProductJoin(
                        brand: String,
                        category: String,
                        color: String,
                        id: Long,
                        name: String,
                        price: Double,
                        timestamp: Long,
                        last_id: Long,
                        last_timestamp: Long)


