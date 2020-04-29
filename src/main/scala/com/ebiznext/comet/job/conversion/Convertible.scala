package com.ebiznext.comet.job.conversion

trait Convertible[A, B] extends (A => B)

trait ConvertibleWith[A, CoA, B] extends ((A, CoA) => B)

package object syntax {

  implicit class ConvertibleOps[A](a: A) {

    def to[B](implicit convert: Convertible[A, B]): B = convert(a)

    def to[B, WithA](adjuvant: WithA)(implicit convert: ConvertibleWith[A, WithA, B]): B =
      convert(a, adjuvant)
  }

}
