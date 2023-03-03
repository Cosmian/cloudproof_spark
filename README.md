# Cloudproof Spark

## Getting Started

1. Install SBT
  - For [Linux](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html), download and extract [ZIP](https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.zip) file
2. Install Spark
  - Download and extract [from source](https://spark.apache.org/downloads.html)
3. Download the CSV file `organizations-100000.csv` from https://www.datablist.com/learn/csv/download-sample-csv-files and put it at the root folder
    ```bash
    wget https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-100000.csv
    ```
4. Execute `sbt assembly && spark-submit --class "CloudproofSpark" --master "local[4]" target/scala-2.12/CloudproofSpark-assembly-1.0.jar`

## Reading the code

- `src/main/scala/com/cosmian/cloudproof/spark/CloudproofSpark.scala` is the main entrypoint, it contains the Spark code to read the CSV, write the encrypted parquet files and read the encrypted parquet files again (with different keys)
- `src/main/java/com/cosmian/cloudproof/spark/CoverCryptCryptoFactory.java` is the class responsible to encrypt/decrypt the files and the columns with CoverCrypt
- `src/main/java/com/cosmian/cloudproof/spark/EncryptionMapping.java` is simple class to encapsulate the mapping in a string form (because Spark config is only working with strings), read it and choose the correct policy for a specific file/column.

## About [parquet format](https://github.com/apache/parquet-format/blob/master/Encryption.md)

## Benchmarks


The size of the CSV file `organizations-100000.csv` is 14MB.

| -                     | Unencrypted | AES256-GCM files encryption | CoverCrypt files and columns encryption |
|-----------------------|-------------|-----------------------------|-----------------------------------------|
| Size of out.parquet   | 14196468    | 18191447                    | 28772947                                |
| Full Timings          | 24s         | 24s                         | 27s                                     |
| Timings on Encryption | 0s          | 0.010529295s                | 3.611999342s                            |

---
Full Timings: boot Spark / read CSV / write Parquet
CPU: Intel(R) Xeon(R) Platinum 8171M CPU @ 2.60GHz

## Testing

You need to have a local spark-3.2.1-bin-hadoop3.3 installation.
See these [instructions](https://spark.apache.org/downloads.html) to download and install.

To test the [TestCloudproof.scala](./src/test/scala/com/cosmian/cloudproof/spark/TestCloudproof.scala), run

```bash
 sbt "test:testOnly *TestCloudproof"
 ```
