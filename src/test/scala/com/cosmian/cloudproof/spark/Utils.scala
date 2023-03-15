package com.cosmian.cloudproof.spark

import com.cosmian.jna.covercrypt.structs.Policy
import com.cosmian.jna.covercrypt.structs.PolicyAxis
import com.cosmian.jna.covercrypt.structs.PolicyAxisAttribute
import com.cosmian.jna.covercrypt.CoverCrypt
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.col
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger

object Utils {
  def log(message: Any): Unit = {
    println("###############################")
    println("###############################")
    println("#################### " + message)
    println("###############################")
    println("###############################")
  }

  def noencryption(
      dataset: String,
      spark: SparkSession
  ): Unit = {
    val df = spark.read
      .option("header", true)
      .csv(dataset)
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
      .parquet("out.parquet." + dataset)
  }

  // Test encryption mappings
  def encryptionMappings(): EncryptionMappings = {
    return (new EncryptionMappings)
      .addPartitionValueMapping("Country=France", "Country::France")
      .addPartitionValueMapping("Country=Germany", "Country::Germany")
      .addPartitionDefaultMapping("Country", "Country::Others")
      .addColumnMapping("Organization Id", "Security::Basic")
  }

  def aesgcm(
      dataset: String,
      spark: SparkSession
  ): Unit = {
    // This config is required for encryption AND decryption
    spark.sparkContext.hadoopConfiguration.set(
      "parquet.crypto.factory.class",
      "com.cosmian.cloudproof.spark.AesGcmCryptoFactory"
    );

    // Here we define the mapping from partition (parquet folders) and columns of our dataset to the CoverCrypt access policies.
    val encryptionMappings = Utils.encryptionMappings()

    val df = spark.read
      .option("header", true)
      .csv(dataset)
      .cache()
      // Here we compute a new column based on the number of employees, this column will then be used to partition the data.
      .withColumn(
        "Size",
        when(col("Number of employees") >= 100, lit("Big"))
          .otherwise(lit("Small"))
      )

    df.write
      // Set our mappings
      .option(
        AesGcmCryptoFactory.ENCRYPTION_MAPPINGS,
        encryptionMappings.toString
      )
      .format("parquet")
      .partitionBy("Country", "Size")
      .mode(SaveMode.Overwrite)
      .parquet("out.parquet." + dataset + ".aes")
  }

  def cloudproof(
      dataset: String,
      spark: SparkSession,
      hybrid: Boolean,
      outputName: String
  ): Unit = {
    CoverCryptCryptoFactory.timings = new AtomicLong(0)
    CoverCryptCryptoFactory.cryptoOverhead = new AtomicLong(0)
    CoverCryptCryptoFactory.files = new AtomicInteger(0)

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
            new PolicyAxisAttribute("Secret", hybrid)
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
    val encryptionMappings = Utils.encryptionMappings()

    val df = spark.read
      .option("header", true)
      .csv(dataset)
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
      .parquet("out.parquet." + dataset + "_" + outputName)

    log(CoverCryptCryptoFactory.files + " files");
    log(
      CoverCryptCryptoFactory.cryptoOverhead
        .get() + " bytes of crypto overhead"
    );
    log(CoverCryptCryptoFactory.timings.get() / 1000000000.0 + "s");
  }
}
