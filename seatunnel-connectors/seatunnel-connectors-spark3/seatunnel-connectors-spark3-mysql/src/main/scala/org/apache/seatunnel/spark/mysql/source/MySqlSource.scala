package org.apache.seatunnel.spark.mysql.source

import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.jdbc.source.Jdbc
import org.apache.seatunnel.spark.mysql.{Config, MySqlUtils}
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}

import scala.collection.JavaConversions.asScalaSet
import scala.util.{Failure, Success, Try}

class MySqlSource extends Jdbc {


  override def getData(env: SparkEnvironment): Dataset[Row] = {
    jdbcReader(env.getSparkSession, Config.MYSQL_DRIVER_CLASS).load()
  }

  override def checkConfig(): CheckResult = {
    MySqlUtils.checkConfig(config, Config.URL, Config.TABLE, Config.USER, Config.PASSWORD)
  }

  override def jdbcReader(sparkSession: SparkSession, driver: String): DataFrameReader = {
    val reader = sparkSession.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", driver)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {
      case Success(options) =>
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })
        reader.options(optionMap)
      case Failure(_) => // do nothing
    }

    reader
  }

  override def getPluginName: String = "MySQL"

}
