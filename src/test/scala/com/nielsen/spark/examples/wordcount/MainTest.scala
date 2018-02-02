package com.nielsen.spark.examples.wordcount

import com.nielsen.spark.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class MainTest extends FunSuite with BeforeAndAfter {

  test("testMain") {
    Main.main(Array("./src/test/resources/data.txt", "./tmp/output"))
  }

  before {
    SparkTestUtils.initTestEnv()
  }

}