# Cloudproof Spark Library

![workflow](https://github.com/Cosmian/cloudproof_spark/actions/workflows/ci.yml/badge.svg)

The Cloudproof Java library provides a Spark-friendly API to [Cosmian's Cloudproof Encryption](https://docs.cosmian.com/cloudproof_encryption/use_cases_benefits/).

Cloudproof Encryption secures data repositories and applications in the cloud with advanced application-level encryption and encrypted search.

<!-- toc -->

- [Licensing](#licensing)
- [Cryptographic primitives](#cryptographic-primitives)
- [Getting Started](#getting-started)
- [Reading the code](#reading-the-code)
- [Parquet format](#parquet-format)
- [Benchmarks](#benchmarks)
  * [Quick summary](#quick-summary)
  * [Parquet without encryption](#parquet-without-encryption)
  * [Parquet with classic AES256-GCM encryption](#parquet-with-classic-aes256-gcm-encryption)
  * [Parquet with CoverCrypt encryption](#parquet-with-covercrypt-encryption)
  * [Parquet with CoverCrypt encryption (post quantum resistant)](#parquet-with-covercrypt-encryption-post-quantum-resistant)
- [Cryptographic overhead](#cryptographic-overhead)
- [Testing](#testing)

<!-- tocstop -->

## Licensing

The library is available under a dual licensing scheme Affero GPL/v3 and commercial. See [LICENSE.md](LICENSE.md) for details.

## Cryptographic primitives

The library is based on:

- [CoverCrypt](https://github.com/Cosmian/cover_crypt) algorithm which allows
creating ciphertexts for a set of attributes and issuing user keys with access
policies over these attributes. `CoverCrypt` offers Post-Quantum resistance.

- [Findex](https://github.com/Cosmian/findex) which is a cryptographic protocol designed to securely make search queries on
an untrusted cloud server. Thanks to its encrypted indexes, large databases can
securely be outsourced without compromising usability.

## Getting Started

1/ Install SBT

- For [Linux](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html), download and extract [ZIP](https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.zip) file

2/ Install Spark

- Download and extract [from source](https://spark.apache.org/downloads.html)

3/ Download the CSV file `organizations-2000000.csv` from <https://www.datablist.com/learn/csv/download-sample-csv-files> and put it at the root folder

```bash
wget https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-2000000.csv
7za x organizations-2000000.csv
```

4/ Execute `sbt assembly && spark-submit --class "CloudproofSpark" --master "local[*]" target/scala-2.12/CloudproofSpark-assembly-1.0.0.jar`

## Reading the code

- `src/main/scala/com/cosmian/cloudproof/spark/CloudproofSpark.scala` is the main entrypoint, it contains the Spark code to read the CSV, write the encrypted parquet files and read the encrypted parquet files again (with different keys)
- `src/main/java/com/cosmian/cloudproof/spark/CoverCryptCryptoFactory.java` is the class responsible to encrypt/decrypt the files and the columns with CoverCrypt
- `src/main/java/com/cosmian/cloudproof/spark/EncryptionMapping.java` is simple class to encapsulate the mapping in a string form (because Spark config is only working with strings), read it and choose the correct policy for a specific file/column.

## Parquet format

Parquet format is described [here](https://github.com/apache/parquet-format/blob/master/Encryption.md).

## Benchmarks

- Timings are about:
  - Spark boot
  - reading CSV dataset
  - writing output in Parquet format
  - <span style="color:red">Du to JVM execution and Spark boot, timings are unstable: given values represent an idea of the performance</span>.
- Parquet Encryption deals with files and columns
- CPU: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
- Datasets are CSV files:
  - 100 000 lines (14M)
  - 500 000 lines (71M)
  - 1 000 000 lines (141M)
  - 2 000 000 lines (283M)
- Size of output regroups all `.parquet` and `.crc` files sizes

### Quick summary

Without post-quantum resistance, `CoverCrypt` scheme overhead size and performance are equivalent to classic symmetric encryption algorithm like *AES256-GCM* but with a hybrid cryptographic system with [multiple benefits](https://docs.cosmian.com/cloudproof_encryption/use_cases_benefits/).

### Parquet without encryption

|                | 100_000 lines | 500_000 lines | 1_000_000 lines | 2_000_000 lines |
|----------------|---------------|---------------|-----------------|-----------------|
| Size of output | 17M           | 66M           | 104M            | 169M            |
| Timings        | 7s            | 24s           | 31s             | 31s             |

### Parquet with classic AES256-GCM encryption

|                | 100_000 lines | 500_000 lines | 1_000_000 lines | 2_000_000 lines |
|----------------|---------------|---------------|-----------------|-----------------|
| Size of output | 19M           | 77M           | 117M            | 183M            |
| Timings        | 6s            | 23s           | 31s             | 36s             |

### Parquet with CoverCrypt encryption

|                | 100_000 lines | 500_000 lines | 1_000_000 lines | 2_000_000 lines |
|----------------|---------------|---------------|-----------------|-----------------|
| Size of output | 20M           | 78M           | 118M            | 185M            |
| Timings        | 9s            | 24s           | 33s             | 40s             |

### Parquet with CoverCrypt encryption (post quantum resistant)

|                | 100_000 lines | 500_000 lines | 1_000_000 lines | 2_000_000 lines |
|----------------|---------------|---------------|-----------------|-----------------|
| Size of output | 21M           | 85M           | 126M            | 192M            |
| Timings        | 9s            | 24s           | 36s             | 42s             |

## Cryptographic overhead

A description of the cryptographic overhead is given [here](./CRYPTOGRAPHIC_OVERHEAD.md).

## Testing

To test the [TestCloudproof.scala](./src/test/scala/com/cosmian/cloudproof/spark/TestCloudproof.scala), run

```bash
sbt "test:testOnly -- -oD"
```
