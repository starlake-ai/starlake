package ai.starlake.utils

import com.fasterxml.jackson.annotation.JsonIgnoreType
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{InjectableValues, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.ghik.silencer.silent

object CometObjectMapper {

  // https://github.com/FasterXML/jackson-databind/issues/962
  @JsonIgnoreType
  private class MixinsForObjectMapper

}

@silent
class CometObjectMapper(
  jf: JsonFactory = null,
  injectables: scala.collection.immutable.Seq[(Class[_], AnyRef)] = Nil
) extends ObjectMapper(jf)
    with ScalaObjectMapper {
  this.registerModule(DefaultScalaModule)
  this.registerModule(CometJacksonModule)

  this.registerModule(
    /* this thing ensures we'll never attempt to serialize the ObjectMapper itself even if it appears in a class
    hierarchy. This happens, in particular, within [[Settings]].
     */
    new SimpleModule
      .setMixInAnnotation(classOf[ObjectMapper], classOf[CometObjectMapper.MixinsForObjectMapper])
  )

  if (injectables.nonEmpty) {
    val iv = new InjectableValues.Std
    injectables.foreach { case (klass, value) => iv.addValue(klass, value) }
    this.setInjectableValues(iv: InjectableValues)
  }
}
