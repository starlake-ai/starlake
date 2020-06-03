package com.ebiznext.comet.utils.conversion

trait Convertible[A, B] extends (A => B)

package object syntax {

  implicit class ConvertibleOps[A](a: A) {
    def to[B](implicit convert: Convertible[A, B]): B = convert(a)
  }

}
