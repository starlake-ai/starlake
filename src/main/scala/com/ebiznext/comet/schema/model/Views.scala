package com.ebiznext.comet.schema.model

/** Thisi tag appears in files and allow import of views and assertions definitions into the current files.
  * @param assertions : List of assertion definitions to include
  * @param views: List of predefined SQL views. useful when views are reused within multiple jobs.
  * @param env : Env variables. These will be used for substitution
  */
case class Views(
  views: Map[String, String] = Map.empty
) {

  def merge(other: Views): Views = {
    this
      .copy(views = views ++ other.views)
  }
}

object Views {

  def merge(other: List[Views]): Views = {
    other.foldLeft(Views()) { (acc, other) =>
      acc.merge(other)
    }
  }
}
