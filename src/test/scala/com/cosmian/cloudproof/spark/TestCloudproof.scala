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
import java.util.concurrent.atomic.AtomicReference

class TestCloudproof extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  override def beforeEach() = {
    spark = SparkSession
      .builder()
      .appName("encrypt with cloudproof")
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

  test("CoverCrypt encryption") {
    val policy = new Policy(
      20,
      Array[PolicyAxis](
        new PolicyAxis(
          "Country",
          Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("France", false),
            new PolicyAxisAttribute("Germany", false),
            new PolicyAxisAttribute("Others", false)
          ),
          false
        ),
        new PolicyAxis(
          "Security",
          Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("None", false),
            new PolicyAxisAttribute("Basic", false),
            new PolicyAxisAttribute("Secret", true)
          ),
          true
        ),
        new PolicyAxis(
          "CompaniesSizes",
          Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("Small", false),
            new PolicyAxisAttribute("Big", false)
          ),
          true
        )
      )
    )
    val masterKeys = CoverCrypt.generateMasterKeys(policy)

    // This config is required for encryption AND decryption
    spark.sparkContext.hadoopConfiguration.set(
      "parquet.crypto.factory.class",
      "com.cosmian.cloudproof.spark.CoverCryptCryptoFactory"
    );

    // Here we define the mapping from partition (parquet folders) and columns of our dataset to the CoverCrypt access policies.
    val encryptionMappings = (new EncryptionMappings)
      .addPartitionValueMapping("Country=France", "Country::France")
      .addPartitionValueMapping("Country=French Guiana", "Country::France")
      .addPartitionValueMapping("Country=French Polynesia", "Country::France")
      .addPartitionValueMapping(
        "Country=French Southern Territories",
        "Country::France"
      )
      .addPartitionValueMapping("Country=Germany", "Country::Germany")
      .addPartitionDefaultMapping("Country", "Country::Others")
      .addPartitionDirectMapping("Size", "CompaniesSizes")
      .addColumnMapping("Organization Id", "Security::Basic")
      .addColumnMapping("Industry", "Security::Secret")

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
      // Set the CoverCrypt master public key
      .option(
        CoverCryptCryptoFactory.COVER_CRYPT_PUBLIC_MASTER_KEY,
        Base64.getEncoder.encodeToString(masterKeys.getPublicKey)
      )
      // Set the policy bytes
      .option(
        CoverCryptCryptoFactory.COVER_CRYPT_POLICY,
        Base64.getEncoder.encodeToString(policy.getBytes)
      )
      // Set our mappings
      .option(
        CoverCryptCryptoFactory.COVER_CRYPT_ENCRYPTION_MAPPINGS,
        encryptionMappings.toString
      )
      .partitionBy("Country", "Size")
      .mode(SaveMode.Overwrite)
      .parquet("out.parquet.cover_crypt")

    Utils.log(CoverCryptCryptoFactory.files);
    Utils.log(CoverCryptCryptoFactory.timings.get() / 1000000000.0 + "s");

  }

}
