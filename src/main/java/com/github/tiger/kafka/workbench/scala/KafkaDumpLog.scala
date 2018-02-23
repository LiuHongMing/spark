package com.github.tiger.kafka.workbench.scala

import kafka.tools.DumpLogSegments

object KafkaDumpLog {

  def main(args: Array[String]): Unit = {

    val args: Array[String] = Array(
      "--files logs/zpcampus1-0/00000000000000000000.log",
      "--print-data-log"
    )

    DumpLogSegments.main(args)
  }

}
