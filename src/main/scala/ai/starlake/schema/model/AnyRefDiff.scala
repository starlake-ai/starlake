package ai.starlake.schema.model

import java.lang.reflect.Modifier

object AnyRefDiff {
  def extractFieldValues(obj: AnyRef): List[NamedValue] = {
    val cls = obj.getClass
    val fields = cls
      .getDeclaredFields()
      .filter(field => !Modifier.isTransient(field.getModifiers))
      .filter(_.getDeclaredAnnotations.isEmpty)
    val fieldNames = fields.map(_.getName)
    cls.getDeclaredMethods.flatMap { method =>
      if (
        fieldNames.contains(method.getName) &&
        Modifier.isPublic(method.getModifiers()) && method.getParameterCount == 0 && !method.getName
          .contains('$')
      ) {
        Some(NamedValue(method.getName, method.invoke(obj)))
      } else None
    }.toList
  }

  def diffListNamed(
    fieldName: String,
    existing: List[Named],
    incoming: List[Named]
  ): ListDiff[Named] = {
    val set1 = existing.toSet
    val set2 = incoming.toSet
    val deleted = Named.diff(set1, set2)
    val added = Named.diff(set2, set1)
    val common1 = Named.diff(set1, deleted).toList.sortBy(_.name)
    val common2 = Named.diff(set2, added).toList.sortBy(_.name)

    val common = common1.zip(common2).map { case (v1, v2) =>
      assert(v1.name == v2.name)
      (v1.name, v1, v2)
    }
    val updated = common.flatMap { case (k, v1, v2) =>
      // diffAny(k, v1, v2).updated
      if (v1.isInstanceOf[NamedValue]) {
        if (v1 != v2)
          List(v1 -> v2)
        else
          Nil
      } else {
        diffAnyRef(k, v1, v2).updated
      }

    }
    ListDiff(fieldName, added.toList, deleted.toList, updated)
  }

  def diffMap(
    fieldName: String,
    existing: Map[String, String],
    incoming: Map[String, String]
  ): ListDiff[Named] = {
    val existingNamed = existing.map { case (k, v) => NamedValue(k, v) }.toList
    val incomingNamed = incoming.map { case (k, v) => NamedValue(k, v) }.toList
    diffListNamed(fieldName, existingNamed, incomingNamed)
  }

  def diffAnyRef(
    fieldName: String,
    existing: AnyRef,
    incoming: AnyRef
  ): ListDiff[Named] = {
    val existingFields = extractFieldValues(existing)
    val incomingFields = extractFieldValues(incoming)
    diffListNamed(fieldName, existingFields, incomingFields)
  }

  def diffOptionAnyRef(
    fieldName: String,
    existing: Option[AnyRef],
    incoming: Option[AnyRef]
  ): ListDiff[Named] = {
    (existing, incoming) match {
      case (Some(existing), Some(incoming)) =>
        diffAnyRef(fieldName, existing, incoming)
      case (None, Some(incoming)) =>
        val incomingFields = extractFieldValues(incoming)
        diffListNamed(fieldName, Nil, incomingFields)
      case (Some(existing), None) =>
        val existingFields = extractFieldValues(existing)
        diffListNamed(fieldName, Nil, existingFields)
      case (None, None) =>
        diffListNamed(fieldName, Nil, Nil)
    }
  }

  def diffOptionString(
    fieldName: String,
    existing: Option[String],
    incoming: Option[String]
  ): ListDiff[String] = {
    diffSetString(fieldName, existing.toSet, incoming.toSet)
  }

  def diffSetString(
    fieldName: String,
    existing: Set[String],
    incoming: Set[String]
  ): ListDiff[String] = {
    val deleted = existing.diff(incoming)
    val added = incoming.diff(existing)
    ListDiff(fieldName, added.toList, deleted.toList, Nil)
  }

  /** @param existing
    * @param incoming
    * @return
    *   (added tables, deleted tables, common tables)
    */
  def partitionNamed[T <: Named](
    existing: List[T],
    incoming: List[T]
  ): (List[T], List[T], List[T]) = {
    val (commonTables, deletedTables) =
      existing.partition(table => incoming.map(_.name.toLowerCase).contains(table.name.toLowerCase))
    val addedTables =
      incoming.filter(table => !existing.map(_.name.toLowerCase).contains(table.name.toLowerCase))
    (addedTables, deletedTables, commonTables)
  }

}

case class ListDiff[T](
  field: String,
  added: List[T],
  deleted: List[T],
  updated: List[(T, T)]
) {
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.isEmpty
}

case class DomainsDiff(added: List[String], deleted: List[String], updated: List[DomainDiff]) {
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())
}

case class JobsDiff(added: List[String], deleted: List[String], updated: List[JobDiff]) {
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())
}

case class ProjectDiff(
  project1: String,
  project2: String,
  domains: DomainsDiff,
  jobsDiff: JobsDiff
) {
  def isEmpty(): Boolean = domains.isEmpty()
}
case class SchemaDiff(
  name: String,
  attributes: ListDiff[Named],
  pattern: ListDiff[String],
  metadata: ListDiff[Named],
  merge: ListDiff[Named],
  comment: ListDiff[String],
  presql: ListDiff[String],
  postsql: ListDiff[String],
  tags: ListDiff[String],
  rls: ListDiff[Named],
  expectations: ListDiff[Named],
  primaryKey: ListDiff[String],
  acl: ListDiff[Named],
  rename: ListDiff[String],
  sample: ListDiff[String],
  filter: ListDiff[String]
) {
  def isEmpty(): Boolean =
    attributes.isEmpty() &&
    attributes.isEmpty() &&
    pattern.isEmpty() &&
    metadata.isEmpty() &&
    merge.isEmpty() &&
    comment.isEmpty() &&
    presql.isEmpty() &&
    postsql.isEmpty() &&
    tags.isEmpty() &&
    rls.isEmpty() &&
    primaryKey.isEmpty() &&
    acl.isEmpty() &&
    rename.isEmpty() &&
    sample.isEmpty() &&
    filter.isEmpty()
}
case class SchemasDiff(added: List[String], deleted: List[String], updated: List[SchemaDiff]) {
  def isEmpty(): Boolean = added.isEmpty && deleted.isEmpty && updated.forall(_.isEmpty())
}

case class DomainDiff(
  name: String,
  tables: SchemasDiff,
  metadata: ListDiff[Named],
  tableRefs: ListDiff[String],
  comment: ListDiff[String],
  tags: ListDiff[String],
  rename: ListDiff[String]
) {
  def isEmpty(): Boolean =
    tables.isEmpty &&
    metadata.isEmpty &&
    tableRefs.isEmpty &&
    comment.isEmpty &&
    tags.isEmpty &&
    rename.isEmpty

}

case class TasksDiff(
  added: List[String],
  deleted: List[String],
  updated: List[ListDiff[Named]]
)

case class JobDiff(
  name: String,
  tasks: TasksDiff,
  taskRefs: ListDiff[String]
) {
  def isEmpty() = taskRefs.isEmpty()
}
