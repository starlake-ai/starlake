package ai.starlake.utils

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.annotation.{JsonAnySetter, JsonIgnoreType}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder
import com.fasterxml.jackson.databind.deser.Deserializers
import com.fasterxml.jackson.databind.ser.Serializers
import com.fasterxml.jackson.module.scala.JacksonModule
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import StorageLevel._

/** This module handles some specific type serializers in a central way (so we don't need to pepper
  * the model code with annotations)
  */
trait CometJacksonModule extends JacksonModule {

  override def getModuleName(): String = "CometJacksonModule"

  this += CometJacksonModuleContents.CometDeserializers
  this += CometJacksonModuleContents.CometSerializers

}

object CometJacksonModule extends CometJacksonModule {

  private val jacksonProtectedSingletons =
    scala.collection.concurrent.TrieMap[Class[_], (AnyRef, Array[StackTraceElement])]()

  /** A trait to identify case objects are requiring special protection from Jackson's deserializer
    *
    * Jackson doesn't know a class is actually an object type, and will happily make new instances
    * of things supposed to be objects (singletons). These 'clone' instances are toxic as they will
    * **not** patmat correctly, as Scala simply compares identityHashcodes in patterns involving a
    * case object.
    *
    * To avoid this, we define this trait, which registers the canonical instance within a special
    * table, and provide a helpful exception
    *
    * One should also define a Builder inheriting from
    * [[ai.starlake.utils.CometJacksonModule.ProtectedSingletonBuilder]], and decorate the case
    * object with @JsonDeserialize(builder = classOf[MyObjectBuilder]).
    *
    * The goal of that builder is to "lie" to Jackson by 'building' an instance, which is actually
    * *the* instance
    *
    * @see
    *   https://github.com/FasterXML/jackson-module-scala/issues/211
    */
  trait JacksonProtectedSingleton {

    private val (registeredInstance, theirBuildStack) =
      jacksonProtectedSingletons.getOrElseUpdate(
        this.getClass,
        (this, new Throwable().getStackTrace)
      )
    if (registeredInstance ne this) {
      throw new IllegalStateException(
        s"Attempting to start a new instance of object (singleton)! \n" +
        s"    Have you decorated your case object with @JsonDeserialize(builder = classOf[${this.getClass.getSimpleName
            .stripSuffix("$")}Builder]) ?\n" +
        s"    The older instance was built at ${theirBuildStack.map(_.toString).mkString("\n")}\n" +
        s"    We are at: "
      )
    }
  }

  /** a base class for fake 'builders' whose purpose is to drive Jackson off attempting to build new
    * instances of case objects upon deserialization.
    *
    * This class will work to recover *the* canonical instance of the `T` class, and return that
    * whenever Jackson requests a 'new' instance
    *
    * @see
    *   https://github.com/FasterXML/jackson-module-scala/issues/211
    */
  @JsonPOJOBuilder()
  abstract class ProtectedSingletonBuilder[T <: JacksonProtectedSingleton: ClassTag]
      extends Serializable {
    private val ourType = implicitly[ClassTag[T]].runtimeClass

    /** A method called by Jackson to "deserialize" all fields of our singleton object.
      *
      * Here our behaviour is to simply ignore anything passed here as we never intend to actually
      * construct a new object.
      */
    @JsonAnySetter
    def withAnything(name: String, value: AnyRef): Unit = {}

    def build: T =
      jacksonProtectedSingletons.get(ourType) match {
        case Some((realSingleton, reallyCreatedThere)) =>
          realSingleton.asInstanceOf[T]
        case None =>
          /* this is the very first time we hear of this type within this Classloader. Perhaps we got hit by lazy
          initialization? This should do the trick.

          Yes, we do depend on private scala internals, but they're widely known nowadays.
           */
          val freshInstance = ourType.getField("MODULE$").get(null).asInstanceOf[T]
          freshInstance
      }
  }

}

private object CometJacksonModuleContents {

  // https://github.com/FasterXML/jackson-databind/issues/962
  @JsonIgnoreType
  class MixinsForObjectMapper

  object StorageLevelSerializer extends JsonSerializer[StorageLevel] {
    override def handledType(): Class[StorageLevel] = classOf[StorageLevel]

    override def serialize(
      value: StorageLevel,
      gen: JsonGenerator,
      serializers: SerializerProvider
    ): Unit = {
      val svalue = value match {
        case NONE                  => "NONE"
        case DISK_ONLY             => "DISK_ONLY"
        case DISK_ONLY_2           => "DISK_ONLY_2"
        case MEMORY_ONLY           => "MEMORY_ONLY"
        case MEMORY_ONLY_2         => "MEMORY_ONLY_2"
        case MEMORY_ONLY_SER       => "MEMORY_ONLY_SER"
        case MEMORY_ONLY_SER_2     => "MEMORY_ONLY_SER_2"
        case MEMORY_AND_DISK       => "MEMORY_AND_DISK"
        case MEMORY_AND_DISK_2     => "MEMORY_AND_DISK_2"
        case MEMORY_AND_DISK_SER   => "MEMORY_AND_DISK_SER"
        case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
        case OFF_HEAP              => "OFF_HEAP"
      }
      gen.writeString(svalue)
    }
  }

  object StorageLevelDeserializer extends JsonDeserializer[StorageLevel] {
    override def handledType(): Class[StorageLevel] = classOf[StorageLevel]

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): StorageLevel = {
      val storageLevel = ctxt.readValue(p, classOf[String])
      StorageLevel.fromString(storageLevel)
    }
  }

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
      (FiniteDurationSerializer :: StorageLevelSerializer :: Nil)
        .map(ser => ser.handledType() -> ser)
        .toMap

    override def findSerializer(
      config: SerializationConfig,
      `type`: JavaType,
      beanDesc: BeanDescription
    ): JsonSerializer[_] = {
      val tpeRawClass = `type`.getRawClass
      val serializer = serializers.get(tpeRawClass).orNull
      serializer
    }
  }

  object CometDeserializers extends Deserializers.Base {

    private val deserializers: Map[Class[_], JsonDeserializer[_]] =
      (FiniteDurationDeserializer :: StorageLevelDeserializer :: Nil)
        .map(ser => ser.handledType() -> ser)
        .toMap

    override def findBeanDeserializer(
      tpe: JavaType,
      config: DeserializationConfig,
      beanDesc: BeanDescription
    ): JsonDeserializer[_] = {
      val tpeRawClass = tpe.getRawClass
      val deserializer = deserializers.get(tpeRawClass).orNull
      deserializer
    }
  }
}
