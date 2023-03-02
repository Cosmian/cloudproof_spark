# Cloudproof Spark

## Getting Started

1. Install SBT
2. Install Spark
3. Download the CSV file `organizations-100000.csv` from https://www.datablist.com/learn/csv/download-sample-csv-files and put it at the root folder
4. Execute `sbt assembly && spark-submit --class "CloudproofSpark" --master "local[4]" target/scala-2.12/CloudproofSpark-assembly-1.0.jar`

## Reading the code

- `src/main/scala/CloudproofSpark.scala` is the main entrypoint, it contains the Spark code to read the CSV, write the encrypted parquet files and read the encrypted parquet files again (with different keys)
- `src/main/scala/com/cosmian/cloudproof/spark/CoverCryptCryptoFactory.java` is the class responsible to encrypt/decrypt the files and the columns with CoverCrypt
- `src/main/scala/com/cosmian/cloudproof/spark/EncryptionMapping.java` is simple class to encapsulate the mapping in a string form (because Spark config is only working with strings), read it and choose the correct policy for a specific file/column.