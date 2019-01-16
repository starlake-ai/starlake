package com.ebiznext.comet.utils

import scala.util.control.NonFatal

object Utils {
  /**
    * Handle tansparently autocloseable resources and correctly chain exceptions
    *
    * @param r : the resource
    * @param f : the try bloc
    * @tparam T : resource Type
    * @tparam V : Try bloc return type
    * @return : Try bloc return value
    */
  def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable,
                                    resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

}
