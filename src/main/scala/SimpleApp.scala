import com.cosmian.cloudproof.spark.{CoverCryptCryptoFactory, PartitionsAttributes}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.cosmian.jna.covercrypt.CoverCrypt
import com.cosmian.jna.covercrypt.structs.{Policy, PolicyAxis, PolicyAxisAttribute}

import java.util.Base64

// Data are from https://www.datablist.com/learn/csv/download-sample-csv-files
object SimpleApp {
  def main(args: Array[String]): Unit = {
    //    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.kms.client.class", "com.cosmian.cloudproof.spark.InMemoryKms")
    //    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.key.list",
    //      "key1: AAECAwQFBgcICQoLDA0ODw==, key2: AAECAAECAAECAAECAAECAA==")
    //    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.column.keys", "key1:Index,Organization Id,Name,Website,Description,Founded,Industry,Number of employees")
    //    spark.sparkContext.hadoopConfiguration.set("parquet.encryption.footer.key", "key2")


    val policy = new Policy(20,
      Array[PolicyAxis](
        new PolicyAxis("Country", Array[PolicyAxisAttribute](
          new PolicyAxisAttribute("France", false),
          new PolicyAxisAttribute("Germany", false),
          new PolicyAxisAttribute("Others", false)
        ), false),
        new PolicyAxis("Department", Array[PolicyAxisAttribute](
          new PolicyAxisAttribute("Base", false),
          new PolicyAxisAttribute("Secret", true)
        ), true)
      )
    )
    val masterKeys = CoverCrypt.generateMasterKeys(policy)
    val decryptionKey = CoverCrypt.generateUserPrivateKey(masterKeys.getPrivateKey, "Country::France", policy);

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val partitionsAttributes = (new PartitionsAttributes)
      .addPartitionValueMapping("Country=France", "Country::France")
      .addPartitionValueMapping("Country=Germany", "Country::Germany")
      .addPartitionDefaultMapping("Country", "Country::Others")

    spark.sparkContext.hadoopConfiguration.set("parquet.crypto.factory.class", "com.cosmian.cloudproof.spark.CoverCryptCryptoFactory");
    spark.sparkContext.hadoopConfiguration.set(CoverCryptCryptoFactory.COVER_CRYPT_PUBLIC_MASTER_KEY, Base64.getEncoder.encodeToString(masterKeys.getPublicKey))
    spark.sparkContext.hadoopConfiguration.set(CoverCryptCryptoFactory.COVER_CRYPT_DECRYPTION_KEY, Base64.getEncoder.encodeToString(decryptionKey))
    spark.sparkContext.hadoopConfiguration.set(CoverCryptCryptoFactory.COVER_CRYPT_POLICY, Base64.getEncoder.encodeToString(policy.getBytes))
    spark.sparkContext.hadoopConfiguration.set(CoverCryptCryptoFactory.COVER_CRYPT_PARTITIONS_ATTRIBUTES, partitionsAttributes.toString)
    spark.sparkContext.hadoopConfiguration.set(CoverCryptCryptoFactory.COVER_CRYPT_COLUMNS_ATTRIBUTES, "Number of employees=Department::Secret,*=Department::Base")

    val df = spark
      .read
      .option("header", true)
      .csv("/home/thibaud/Code/cloudproof_spark/organizations-100000.csv").cache()

    println("#########################")
    println("#########################")
    println("######################### BEFORE")
    println(df.count())
    println("#########################")
    println("#########################")
    println("#########################")

    df
      .write
      .partitionBy("Country")
      .mode(SaveMode.Overwrite)
      .parquet("out.parquet")

    val newDf = spark.read.parquet("out.parquet/Country=France")

    println("#########################")
    println("#########################")
    println("######################### AFTER")
    println(newDf.count())
    println("#########################")
    println("#########################")
    println("#########################")

    spark.stop()
  }
}
