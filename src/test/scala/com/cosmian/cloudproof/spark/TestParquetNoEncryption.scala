package com.cosmian.cloudproof.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite


class TestParquetNoEncryption extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  override def beforeEach() = {
    spark = SparkSession
      .builder()
      .appName("no encryption")
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

  test("No encryption encryption with classic policy: 100_000 enterprises") {
    Utils.noencryption("organizations-100000.csv", spark)
  }

  test("No encryption encryption with classic policy: 500_000 enterprises") {
    Utils.noencryption("organizations-500000.csv", spark)
  }

  test("No encryption encryption with classic policy: 1_000_000 enterprises") {
    Utils.noencryption("organizations-1000000.csv", spark)
  }

  test("No encryption encryption with classic policy: 2_000_000 enterprises") {
    Utils.noencryption("organizations-2000000.csv", spark)
  }

}
