package com.cosmian.cloudproof.spark

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.BinaryType
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.DataFrame
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import scala.reflect.io.Directory
import com.cosmian.rest.kmip.objects.PublicKey
import org.apache.spark.sql.SaveMode
import java.util.Base64
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.Column
import java.util.concurrent.TimeUnit
import com.cosmian.jna.covercrypt.structs.Policy
import com.cosmian.jna.covercrypt.structs.PolicyAxis
import com.cosmian.jna.covercrypt.structs.PolicyAxisAttribute
import com.cosmian.jna.covercrypt.CoverCrypt

class TestParquetAesGcm extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  override def beforeEach() = {
    spark = SparkSession
      .builder()
      .appName("encrypt with AES/GCM/NoPadding")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() = {
    try {
      super.afterEach() // To be stackable, must call super.afterEach
    } finally {
      spark.stop()
    }
  }

  test("AES256-GCM encryption") {
    // This config is required for encryption AND decryption
    spark.sparkContext.hadoopConfiguration.set(
      "parquet.crypto.factory.class",
      "com.cosmian.cloudproof.spark.AesGcmCryptoFactory"
    );

    val df = spark.read
      .option("header", true)
      .csv("organizations-100000.csv")
      .cache()
      // Here we compute a new column based on the number of employees, this column will then be used to partition the data.
      .withColumn(
        "Size",
        when(col("Number of employees") >= 100, lit("Big"))
          .otherwise(lit("Small"))
      )

    df.write
      .format("parquet")
      .partitionBy("Country", "Size")
      .mode(SaveMode.Overwrite)
      .parquet("out.parquet.aesgcm")

    Utils.log(AesGcmCryptoFactory.files);
    Utils.log(AesGcmCryptoFactory.timings.get() / 1000000000.0 + "s");

  }

}
