package com.ebiznext.comet.schema.model

/** Thisi tag appears in files and allow import of views and assertions definitions into the current files.
  * @param assertions : List of assertion definitions to include
  * @param views: List of predefined SQL views. useful when views are reused within multiple jobs.
  * @param env : Env variables. These will be used for substitution
  */
case class Include(
  assertions: Map[String, String] = Map.empty,
  views: Map[String, String] = Map.empty,
  env: Map[String, Map[String, String]] = Map.empty
) {
  val assertionDefinitions: AssertionDefinitions = AssertionDefinitions(assertions)

  def merge(include: Include): Include = {
    this
      .copy(views = views ++ include.views)
      .copy(assertions = assertions ++ include.assertions)
  }
}

object Include {

  def merge(includes: List[Include]): Include = {
    includes.foldLeft(Include()) { (acc, include) =>
      acc.merge(include)
    }
  }
}
