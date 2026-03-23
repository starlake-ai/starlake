package ai.starlake.utils

/** Spark-free replacement for org.apache.spark.sql.catalyst.util.CaseInsensitiveMap.
  * A Map wrapper that performs case-insensitive key lookups while preserving the original key casing.
  */
class CaseInsensitiveMap[V] private (val original: Map[String, V])
    extends Map[String, V]
    with Serializable {

  private val lowercaseMap: Map[String, V] =
    original.map { case (k, v) => k.toLowerCase -> v }

  override def get(key: String): Option[V] = lowercaseMap.get(key.toLowerCase)

  override def contains(key: String): Boolean = lowercaseMap.contains(key.toLowerCase)

  override def iterator: Iterator[(String, V)] = original.iterator

  override def updated[V1 >: V](key: String, value: V1): Map[String, V1] =
    new CaseInsensitiveMap(original.updated(key, value.asInstanceOf[V]))

  override def removed(key: String): Map[String, V] = {
    val lowerKey = key.toLowerCase
    val newOriginal = original.filterNot { case (k, _) => k.toLowerCase == lowerKey }
    new CaseInsensitiveMap(newOriginal)
  }

}

object CaseInsensitiveMap {
  def apply[V](map: Map[String, V]): CaseInsensitiveMap[V] =
    new CaseInsensitiveMap(map)

  def empty[V]: CaseInsensitiveMap[V] =
    new CaseInsensitiveMap(Map.empty[String, V])
}