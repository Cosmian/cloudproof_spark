import com.cosmian.cloudproof.spark.{CoverCryptCryptoFactory, EncryptionMappings}
import com.cosmian.jna.covercrypt.CoverCrypt
import com.cosmian.jna.covercrypt.structs.{Policy, PolicyAxis, PolicyAxisAttribute}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Base64

// `organizations-100000.csv` is from https://www.datablist.com/learn/csv/download-sample-csv-files
object CloudproofSpark {
  def main(args: Array[String]): Unit = {
    val policy = new Policy(20,
      Array[PolicyAxis](
        new PolicyAxis("Country", Array[PolicyAxisAttribute](
          new PolicyAxisAttribute("France", false),
          new PolicyAxisAttribute("Germany", false),
          new PolicyAxisAttribute("Others", false)
        ), false),
        new PolicyAxis("Security", Array[PolicyAxisAttribute](
          new PolicyAxisAttribute("None", false),
          new PolicyAxisAttribute("Basic", false),
          new PolicyAxisAttribute("Secret", true)
        ), true),
        new PolicyAxis("CompaniesSizes", Array[PolicyAxisAttribute](
          new PolicyAxisAttribute("Small", false),
          new PolicyAxisAttribute("Big", false)
        ), true)
      )
    )
    val masterKeys = CoverCrypt.generateMasterKeys(policy)

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    // This config is required for encryption AND decryption
    spark.sparkContext.hadoopConfiguration.set("parquet.crypto.factory.class", "com.cosmian.cloudproof.spark.CoverCryptCryptoFactory");

    // Here we define the mapping from partition (parquet folders) and columns of our dataset to the CoverCrypt access policies.
    val encryptionMappings = (new EncryptionMappings)
      .addPartitionValueMapping("Country=France", "Country::France")
      .addPartitionValueMapping("Country=French Guiana", "Country::France")
      .addPartitionValueMapping("Country=French Polynesia", "Country::France")
      .addPartitionValueMapping("Country=French Southern Territories", "Country::France")
      .addPartitionValueMapping("Country=Germany", "Country::Germany")
      .addPartitionDefaultMapping("Country", "Country::Others")
      .addPartitionDirectMapping("Size", "CompaniesSizes")
      .addColumnMapping("Organization Id", "Security::Basic")
      .addColumnMapping("Industry", "Security::Secret")

    val df = spark
      .read
      .option("header", true)
      .csv("organizations-100000.csv")
      .cache()
      // Here we compute a new column based on the number of employees, this column will then be used to partition the data.
      .withColumn("Size", when(col("Number of employees") >= 100, lit("Big")).otherwise(lit("Small")))

    df
      .write
      // Set the CoverCrypt master public key
      .option(CoverCryptCryptoFactory.COVER_CRYPT_PUBLIC_MASTER_KEY, Base64.getEncoder.encodeToString(masterKeys.getPublicKey))
      // Set the policy bytes
      .option(CoverCryptCryptoFactory.COVER_CRYPT_POLICY, Base64.getEncoder.encodeToString(policy.getBytes))
      // Set our mappings
      .option(CoverCryptCryptoFactory.COVER_CRYPT_ENCRYPTION_MAPPINGS, encryptionMappings.toString)
      .partitionBy("Country", "Size")
      .mode(SaveMode.Overwrite)
      .parquet("out.parquet")

    val smallFrenchCompaniesDecryptionKey = CoverCrypt.generateUserPrivateKey(masterKeys.getPrivateKey, "Country::France && CompaniesSizes::Small && Security::None", policy);
    val result1 = spark.read
      .option(CoverCryptCryptoFactory.COVER_CRYPT_DECRYPTION_KEY, Base64.getEncoder.encodeToString(smallFrenchCompaniesDecryptionKey))
      // We only read France / Small files following our decryption key access policy
      .parquet(
        "out.parquet/Country=France/Size=Small",
        "out.parquet/Country=French Guiana/Size=Small",
        "out.parquet/Country=French Polynesia/Size=Small",
        "out.parquet/Country=French Southern Territories/Size=Small"
      )
      // We also drop the two secured columns (since we are Security::None we cannot read them)
      .drop("Organization Id")
      .drop("Industry")
      .first()
    log(result1);

    try {
      spark.read
        .option(CoverCryptCryptoFactory.COVER_CRYPT_DECRYPTION_KEY, Base64.getEncoder.encodeToString(smallFrenchCompaniesDecryptionKey))
        .parquet("out.parquet/Country=Germany/Size=Small") // Note, we query the Germany small companies here.
        .drop("Organization Id")
        .drop("Industry")
        .first()

      throw new RuntimeException("Should have throw before this line!")
    } catch {
      case e: SparkException => log("Cannot decrypt the files containing small german companies.")
    }

    try {
      spark.read
        .option(CoverCryptCryptoFactory.COVER_CRYPT_DECRYPTION_KEY, Base64.getEncoder.encodeToString(smallFrenchCompaniesDecryptionKey))
        .parquet("out.parquet/Country=France/Size=Small")
        .first() // Note, we do not drop the secured column here.

      throw new RuntimeException("Should have throw before this line!")
    } catch {
      case e: SparkException => log("Cannot decrypt `Organization Id` and `Industry` columns for small french companies.")
    }

    // With more access, we can decrypt all the France folder (big and small companies) and see the `Organization Id` and `Industry` column.
    val secretAllFrenchCompaniesDecryptionKey = CoverCrypt.generateUserPrivateKey(masterKeys.getPrivateKey, "Country::France && CompaniesSizes::Big && Security::Secret", policy);
    val result2 = spark.read
      .option(CoverCryptCryptoFactory.COVER_CRYPT_DECRYPTION_KEY, Base64.getEncoder.encodeToString(secretAllFrenchCompaniesDecryptionKey))
      // We only read France (but our key allow for the other france partition)
      .parquet("out.parquet/Country=France")
      // Note we do not drop the secured column here.
      .first()
    log(result2);

    spark.stop()

    log(CoverCryptCryptoFactory.files);
    log(CoverCryptCryptoFactory.timings.get() / 1000000000.0 + "s");
  }

  private def log(message: Any): Unit = {
    println("###############################")
    println("###############################")
    println("#################### " + message)
    println("###############################")
    println("###############################")
  }
}
