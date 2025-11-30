package ai.starlake.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.hadoop.fs.Path

class HadoopModule extends SimpleModule {
  class HadoopPathSerializer(pathClass: Class[Path]) extends StdSerializer[Path](pathClass) {
    def this() = {
      this(classOf[Path])
    }

    override def serialize(value: Path, jGen: JsonGenerator, provider: SerializerProvider): Unit = {
      jGen.writeString(value.toString)
    }
  }
  this.addSerializer(new HadoopPathSerializer())
}
