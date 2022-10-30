package ai.starlake.job.transform

import scala.annotation.tailrec

case class TaskViewDependencyNode(data: TaskViewDependency, children: List[TaskViewDependencyNode])

object TaskViewDependencyNode {

  def dependencyGraph(nodes: List[TaskViewDependency]): List[TaskViewDependencyNode] = {
    val nodesByParent = nodes.groupBy(_.parent)
    val topNodes = nodes.filter(node => !nodes.exists(_.name == node.parent))
    val bottomToTop = sortNodesByDepth(topNodes, nodesByParent)
    val maxDepth = bottomToTop.headOption.map(_.depth).getOrElse(0)
    buildFromBottom(maxDepth, bottomToTop, nodesByParent, Map.empty)
  }

  private case class EntryWithDepth(data: TaskViewDependency, depth: Int)

  @tailrec
  private def sortNodesByDepth(
    depth: Int,
    nodesInDepth: List[TaskViewDependency],
    nodesByParent: Map[String, List[TaskViewDependency]],
    acc: List[EntryWithDepth]
  ): List[EntryWithDepth] = {
    val withDepth = nodesInDepth.map(dep => EntryWithDepth(dep, depth))
    val calculated = withDepth ++ acc // accumulating in reverse order
    val children = nodesInDepth.flatMap(dep => nodesByParent.getOrElse(dep.name, Nil))
    if (children.isEmpty)
      calculated
    else
      sortNodesByDepth(depth + 1, children, nodesByParent, calculated)
  }

  private def sortNodesByDepth(
    nodesInDepth: List[TaskViewDependency],
    nodesByParent: Map[String, List[TaskViewDependency]]
  ): List[EntryWithDepth] =
    sortNodesByDepth(0, nodesInDepth, nodesByParent, Nil)

  @tailrec
  private def buildFromBottom(
    depth: Int,
    remaining: Seq[EntryWithDepth],
    nodesByParent: Map[String, List[TaskViewDependency]],
    processedNodesById: Map[String, TaskViewDependencyNode]
  ): List[TaskViewDependencyNode] = {
    val (nodesOnCurrentDepth, rest) = remaining.span(_.depth == depth)
    val newProcessedNodes = nodesOnCurrentDepth.map { n =>
      val nodeId = n.data.name
      val children =
        nodesByParent
          .getOrElse(nodeId, Nil)
          .flatMap(c => processedNodesById.get(c.name))
      nodeId -> TaskViewDependencyNode(n.data, children)
    }.toMap
    if (depth > 0) {
      buildFromBottom(depth - 1, rest, nodesByParent, processedNodesById ++ newProcessedNodes)
    } else {
      // top nodes
      newProcessedNodes.values.toList
    }
  }
}
