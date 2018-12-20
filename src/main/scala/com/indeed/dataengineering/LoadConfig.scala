package com.indeed.dataengineering

import com.indeed.dataengineering.config.AnalyticsTaskConfig

object LoadConfig {
  var conf: Map[String, String] = Map()

  def apply(args: Array[String]): Map[String, String] = {
    conf = AnalyticsTaskConfig.parseCmdLineArguments(args)
    conf
  }
}
