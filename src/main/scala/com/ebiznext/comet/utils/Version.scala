package com.ebiznext.comet.utils

case class Version(var version: String) extends Comparable[Version] {
  if (version == null) throw new IllegalArgumentException("Version can not be null")
  if (!version.matches("[0-9]+(\\.[0-9]+)*"))
    throw new IllegalArgumentException("Invalid version format")

  override def compareTo(that: Version): Int = {
    if (that == null) return 1
    val thisParts = version.split("\\.")
    val thatParts = that.version.split("\\.")
    val length = Math.max(thisParts.length, thatParts.length)
    for (i <- 0 until length) {
      val thisPart =
        if (i < thisParts.length) thisParts(i).toInt
        else 0
      val thatPart =
        if (i < thatParts.length) thatParts(i).toInt
        else 0
      if (thisPart < thatPart) return -1
      if (thisPart > thatPart) return 1
    }
    0
  }

  override def equals(that: Any): Boolean = {
    if (that == null)
      false
    else if (!that.isInstanceOf[Version])
      false
    else
      this.compareTo(that.asInstanceOf[Version]) == 0
  }
}
