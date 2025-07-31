package ai.starlake.config

import ai.starlake.config.Settings.JdbcEngine.TableDdl
import ai.starlake.config.Settings._
import ai.starlake.job.ingest.RejectedRecord
import ai.starlake.schema.model.{BigQuerySink, EsSink, FsSink, JdbcSink, Mode, Sink}
import ai.starlake.utils.TransformEngine
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel

class KryoSerialization extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Settings])
    kryo.register(classOf[AppConfig])
    kryo.register(classOf[Metrics])
    kryo.register(classOf[Audit])
    kryo.register(classOf[Elasticsearch])
    kryo.register(classOf[Internal])
    kryo.register(classOf[Lock])
    kryo.register(classOf[ConnectionInfo])
    kryo.register(classOf[AccessPolicies])
    kryo.register(classOf[KafkaConfig])
    kryo.register(classOf[ExpectationsConfig])
    kryo.register(classOf[Sink])
    kryo.register(classOf[BigQuerySink])
    kryo.register(classOf[EsSink])
    kryo.register(classOf[FsSink])
    kryo.register(classOf[JdbcSink])
    kryo.register(classOf[TableDdl])
    kryo.register(classOf[KafkaTopicConfig])
    kryo.register(classOf[Mode])
    kryo.register(classOf[StorageLevel])
    kryo.register(classOf[TransformEngine])
    kryo.register(classOf[RejectedRecord])

  }
}
