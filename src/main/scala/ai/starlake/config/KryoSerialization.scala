package ai.starlake.config

import ai.starlake.config.Settings.JdbcEngine.TableDdl
import ai.starlake.config.Settings._
import ai.starlake.job.ingest.RejectedRecord
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.model.{BigQuerySink, EsSink, FsSink, JdbcSink, Mode, NoneSink, Sink}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel

class KryoSerialization extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Settings])
    kryo.register(classOf[Comet])
    kryo.register(classOf[Metrics])
    kryo.register(classOf[Audit])
    kryo.register(classOf[Elasticsearch])
    kryo.register(classOf[Internal])
    kryo.register(classOf[Connection])
    kryo.register(classOf[AccessPolicies])
    kryo.register(classOf[KafkaConfig])
    kryo.register(classOf[Expectations])
    kryo.register(classOf[Sink])
    kryo.register(classOf[BigQuerySink])
    kryo.register(classOf[EsSink])
    kryo.register(classOf[FsSink])
    kryo.register(classOf[NoneSink])
    kryo.register(classOf[JdbcSink])
    kryo.register(classOf[TableDdl])
    kryo.register(classOf[KafkaTopicConfig])
    kryo.register(classOf[Mode])
    kryo.register(classOf[StorageLevel])
    kryo.register(classOf[PrivacyEngine])
    kryo.register(classOf[RejectedRecord])

  }
}
