package com.ebiznext.comet.utils

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{BeanDescription, DeserializationConfig, DeserializationContext, JavaType, JsonDeserializer, JsonSerializer, SerializationConfig, SerializerProvider}
import com.fasterxml.jackson.databind.deser.Deserializers
import com.fasterxml.jackson.databind.ser.Serializers
import com.fasterxml.jackson.module.scala.JacksonModule

import scala.concurrent.duration.FiniteDuration

/**
  * This module handles some specific type serializers in a central way (so we don't need to pepper the model code
  * with annotations)
  */
trait CometJacksonModule extends JacksonModule {

  override def getModuleName(): String = "CometJacksonModule"

  this += CometJacksonModuleContents.CometDeserializers
  this += CometJacksonModuleContents.CometSerializers
}

object CometJacksonModule extends CometJacksonModule

private object CometJacksonModuleContents {

  object FiniteDurationSerializer extends JsonSerializer[FiniteDuration] {


    override def handledType(): Class[FiniteDuration] = classOf[FiniteDuration]

    override def serialize(
                            value: FiniteDuration,
                            gen: JsonGenerator,
                            serializers: SerializerProvider
                          ): Unit = {
      gen.writeNumber(value.toMillis)
    }
  }

  object FiniteDurationDeserializer extends JsonDeserializer[FiniteDuration] {
    override def handledType(): Class[FiniteDuration] = classOf[FiniteDuration]

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): FiniteDuration = {
      val milliseconds = ctxt.readValue(p, classOf[Long])
      FiniteDuration.apply(milliseconds, TimeUnit.MILLISECONDS)
    }
  }

  object CometSerializers extends Serializers.Base {
    private val serializers: Map[Class[_], JsonSerializer[_]] =
      (FiniteDurationSerializer :: Nil).map(ser => ser.handledType() -> ser).toMap

    override def findSerializer(config: SerializationConfig, `type`: JavaType, beanDesc: BeanDescription): JsonSerializer[_] = {
      val tpeRawClass = `type`.getRawClass
      val serializer =  serializers.get(tpeRawClass).orNull
      serializer
    }
  }

  object CometDeserializers extends Deserializers.Base {
    private val deserializers: Map[Class[_], JsonDeserializer[_]] =
      (FiniteDurationDeserializer :: Nil).map(ser => ser.handledType() -> ser).toMap

    override def findBeanDeserializer(tpe: JavaType, config: DeserializationConfig, beanDesc: BeanDescription): JsonDeserializer[_] = {
      val tpeRawClass = tpe.getRawClass
      val deserializer =  deserializers.get(tpeRawClass).orNull
      deserializer
    }
  }
}