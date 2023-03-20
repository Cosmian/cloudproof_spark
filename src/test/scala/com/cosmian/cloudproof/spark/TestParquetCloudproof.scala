package com.cosmian.cloudproof.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class TestParquetCloudproof extends AnyFunSuite with BeforeAndAfterEach {

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

  test("CoverCrypt encryption with classic policy: 100_000 enterprises") {
    Utils.cloudproof(
      "organizations-100000.csv",
      spark,
      false,
      "cloudproof_classic"
    )
  }
  test("CoverCrypt encryption with hybrid policy: 100_000 enterprises") {
    Utils.cloudproof(
      "organizations-100000.csv",
      spark,
      true,
      "cloudproof_hybrid"
    )
  }

  test("CoverCrypt encryption with classic policy: 500_000 enterprises") {
    Utils.cloudproof(
      "organizations-500000.csv",
      spark,
      false,
      "cloudproof_classic"
    )
  }
  test("CoverCrypt encryption with hybrid policy: 500_000 enterprises") {
    Utils.cloudproof(
      "organizations-500000.csv",
      spark,
      true,
      "cloudproof_hybrid"
    )
  }

  test("CoverCrypt encryption with classic policy: 1_000_000 enterprises") {
    Utils.cloudproof(
      "organizations-1000000.csv",
      spark,
      false,
      "cloudproof_classic"
    )
  }
  test("CoverCrypt encryption with hybrid policy: 1_000_000 enterprises") {
    Utils.cloudproof(
      "organizations-1000000.csv",
      spark,
      true,
      "cloudproof_hybrid"
    )
  }

  test("CoverCrypt encryption with classic policy: 2_000_000 enterprises") {
    Utils.cloudproof(
      "organizations-2000000.csv",
      spark,
      false,
      "cloudproof_classic"
    )
  }
  test("CoverCrypt encryption with hybrid policy: 2_000_000 enterprises") {
    Utils.cloudproof(
      "organizations-2000000.csv",
      spark,
      true,
      "cloudproof_hybrid"
    )
  }

}
