package com.nielsen.spark.test

import java.io.File

import org.apache.commons.exec.{CommandLine, DefaultExecutor}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils

object SparkTestUtils {
  def initTestEnv() {
    val user_dir = System.getProperty("user.dir").replace('\\', '/')
    val spark_hive_warehouse_dir = s"file:///$user_dir/tmp/spark-hive"

    sys.props.put("hadoop.home.dir", s"$user_dir/hadoop")
    sys.props.put("hive.exec.scratchdir", s"$user_dir/tmp/hive/scratch")
    sys.props.put("spark.sql.warehouse.dir", spark_hive_warehouse_dir)
    sys.props.put("spark.local.dir", s"$user_dir/tmp/spark/scratch")
    sys.props.put("spark.master", "local")
    sys.props.put("spark.sql.shuffle.partitions", "1")
    //sys.props.put("spark.master", "local[4]")
    //sys.props.put("spark.sql.shuffle.partitions", "4")
    //sys.props.put("spark.master", "local[*]")
    //sys.props.put("spark.sql.shuffle.partitions", "8")

    initTmpFolder(user_dir)
  }

  private def initTmpFolder(user_dir: String): Unit = {
    try {
      val tmp_folder = s"$user_dir/tmp"
      val file = new File(tmp_folder)
      if (file.exists) FileUtils.deleteDirectory(file)
      file.mkdir
      var line = s"chmod -R 777 $tmp_folder"
      val IS_WINDOWS = SystemUtils.IS_OS_WINDOWS
      if (IS_WINDOWS) line = s"$user_dir/hadoop/bin/winutils.exe $line"
      val cmd_line = CommandLine.parse(line)
      val executor = new DefaultExecutor
      val exit_value = executor.execute(cmd_line)
      if (exit_value != 0) throw new Exception(s"Error while executing chmod on tmp folder => $tmp_folder")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}