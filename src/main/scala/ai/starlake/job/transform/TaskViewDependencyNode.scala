package ai.starlake.job.transform

case class TaskViewDependencyNode(
  data: TaskViewDependency,
  children: List[TaskViewDependencyNode]
) {
  def print(level: Int = 0): Unit = {
    println("  " * level + data.name)
    children.foreach(_.print(level + 1))
  }
  def isTask(): Boolean = data.typ == "task"
}

object TaskViewDependencyNode {

  def dependencies(
    entities: List[TaskViewDependency],
    relations: List[TaskViewDependency]
  ): List[TaskViewDependencyNode] = {
    val result = entities.map { entity =>
      dependencies(entity, entities, relations)
    }
    result
  }

  def dependencies(
    entity: TaskViewDependency,
    entities: List[TaskViewDependency],
    relations: List[TaskViewDependency]
  ): TaskViewDependencyNode = {
    val thisEntityRelations =
      relations
        .filter(_.name == entity.name)
        .groupBy(x => x.name + "." + x.parent)
        .mapValues(_.head)
        .values
        .toList
    val parentEntities = thisEntityRelations.flatMap { r =>
      entities.find(_.name == r.parent)
    }
    val deps = parentEntities.map { parentEntity =>
      dependencies(parentEntity, entities, relations)
    }
    TaskViewDependencyNode(entity, deps)
  }
}
