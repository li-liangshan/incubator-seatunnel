package org.apache.seatunnel.spark.mysql.sink

import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.jdbc.sink.Jdbc
import org.apache.seatunnel.spark.mysql.Config
import org.apache.spark.sql.execution.datasources.jdbc.v2.JDBCSaveMode
import org.apache.spark.sql.{Dataset, Row}

class MySqlSink extends Jdbc {


  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val saveMode = config.getString("saveMode")
    val map = Map(
      "saveMode" -> JDBCSaveMode.Update.toString,
      "driver" -> Config.MYSQL_DRIVER_CLASS,
      "url" -> config.getString("url"),
      "user" -> config.getString("user"),
      "password" -> config.getString("password"),
      "dbtable" -> config.getString("dbTable"),
      "useSsl" -> config.getString("useSsl"),
      "isolationLevel" -> config.getString("isolationLevel"),
      "customUpdateStmt" -> config.getString("customUpdateStmt"),
      "duplicateIncs" -> config.getString("duplicateIncs"),
      "showSql" -> config.getString("showSql"))

    if ("update".equals(saveMode)) {
      data.write
        .format("org.apache.spark.sql.execution.datasources.jdbc2")
        .options(map)
        .save()
    } else {
      val prop = new java.util.Properties()
      prop.setProperty("driver", Config.MYSQL_DRIVER_CLASS)
      prop.setProperty("user", config.getString("user"))
      prop.setProperty("password", config.getString("password"))
      data.write
        .mode(saveMode)
        .jdbc(config.getString("url"), config.getString("dbTable"), prop)
    }
  }


  override def getPluginName: String = "MySQL"

}
