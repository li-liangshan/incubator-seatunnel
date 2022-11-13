package org.apache.seatunnel.spark.mysql

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.Config

object MySqlUtils {

  def checkConfig(config: Config, params: String*): CheckResult = {
    if (config.hasPath(Config.URL) && !config.getString(Config.URL).trim.startsWith(Config.MYSQL_URL_PREFIX)) {
      return CheckResult.error("url must be started with jdbc:mysql")
    }
    checkAllExists(config, params: _*)
  }

}
