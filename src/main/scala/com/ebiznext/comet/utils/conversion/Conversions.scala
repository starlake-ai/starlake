package com.ebiznext.comet.utils.conversion

import org.apache.hadoop.fs.RemoteIterator

import scala.language.implicitConversions

object Conversions {

  /** Make us able to use a java Hadoop Iterator as a scala Iterator Making us able to use filter,
    * map ...
    * @param underlying
    *   the java Hadoop Iterator
    * @tparam T
    * @return
    *   the augmented iterator
    */
  implicit def convertToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext = underlying.hasNext

      override def next = underlying.next
    }
    wrapper(underlying)
  }
}
