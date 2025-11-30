package ai.starlake.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.{
  DISK_ONLY,
  DISK_ONLY_2,
  DISK_ONLY_3,
  MEMORY_AND_DISK,
  MEMORY_AND_DISK_2,
  MEMORY_AND_DISK_SER,
  MEMORY_AND_DISK_SER_2,
  MEMORY_ONLY,
  MEMORY_ONLY_2,
  MEMORY_ONLY_SER,
  MEMORY_ONLY_SER_2,
  NONE,
  OFF_HEAP
}

class StorageLevelModule extends SimpleModule {
  class StorageLevelSerializer(storageLevelClass: Class[StorageLevel])
      extends StdSerializer[StorageLevel](storageLevelClass) {
    def this() = {
      this(classOf[StorageLevel])
    }

    override def serialize(
      value: StorageLevel,
      jGen: JsonGenerator,
      provider: SerializerProvider
    ): Unit = {
      def toString(s: StorageLevel): String = s match {
        case NONE                  => "NONE"
        case DISK_ONLY             => "DISK_ONLY"
        case DISK_ONLY_2           => "DISK_ONLY_2"
        case DISK_ONLY_3           => "DISK_ONLY_3"
        case MEMORY_ONLY           => "MEMORY_ONLY"
        case MEMORY_ONLY_2         => "MEMORY_ONLY_2"
        case MEMORY_ONLY_SER       => "MEMORY_ONLY_SER"
        case MEMORY_ONLY_SER_2     => "MEMORY_ONLY_SER_2"
        case MEMORY_AND_DISK       => "MEMORY_AND_DISK"
        case MEMORY_AND_DISK_2     => "MEMORY_AND_DISK_2"
        case MEMORY_AND_DISK_SER   => "MEMORY_AND_DISK_SER"
        case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
        case OFF_HEAP              => "OFF_HEAP"
        case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
      }
      jGen.writeString(toString(value))
    }
  }
  this.addSerializer(new StorageLevelSerializer())
}
