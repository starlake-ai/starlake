package com.ebiznext.comet.db

/**
 * Created by Mourad on 31/07/2018.
 */
trait DBConnection {
  def close(): Unit
  def read[V <: AnyRef](key: String)(implicit m: Manifest[V]): Option[V]
  def write[V <: AnyRef](key: String, value: V)(implicit m: Manifest[V]): Unit
  def delete(key: String): Unit
}
