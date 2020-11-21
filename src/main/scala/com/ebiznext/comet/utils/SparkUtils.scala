package com.ebiznext.comet.utils

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.{
  DISK_ONLY,
  DISK_ONLY_2,
  MEMORY_AND_DISK,
  MEMORY_AND_DISK_2,
  MEMORY_AND_DISK_SER,
  MEMORY_AND_DISK_SER_2,
  MEMORY_ONLY,
  MEMORY_ONLY_2,
  MEMORY_ONLY_SER,
  MEMORY_ONLY_SER_2,
  OFF_HEAP
}

object SparkUtils {

  def storageLevel(str: String): StorageLevel = str match {
    case "DISK_ONLY"             => DISK_ONLY
    case "DISK_ONLY_2"           => DISK_ONLY_2
    case "MEMORY_ONLY"           => MEMORY_ONLY
    case "MEMORY_ONLY_2"         => MEMORY_ONLY_2
    case "MEMORY_ONLY_SER"       => MEMORY_ONLY_SER
    case "MEMORY_ONLY_SER_2"     => MEMORY_ONLY_SER_2
    case "MEMORY_AND_DISK_2"     => MEMORY_AND_DISK_2
    case "MEMORY_AND_DISK_SER"   => MEMORY_AND_DISK_SER
    case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
    case "OFF_HEAP"              => OFF_HEAP
    case _                       => MEMORY_AND_DISK
  }

}
