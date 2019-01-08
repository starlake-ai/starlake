package com.ebiznext.comet.schema

import org.apache.hadoop.fs.RemoteIterator

package object handlers {
  implicit def convertToScalaIterator[T](
                                          underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext = underlying.hasNext

      override def next = underlying.next
    }
    wrapper(underlying)
  }
}
