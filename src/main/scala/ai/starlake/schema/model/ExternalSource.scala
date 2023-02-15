package ai.starlake.schema.model

case class ExternalSource(
  projects: List[ExternalProject]
)

case class ExternalProject(project: String, datasets: List[ExternalDataset] = Nil) {
  def toMap(): Map[String, List[String]] =
    datasets.map { dataset => dataset.dataset -> dataset.tables }.toMap
}

case class ExternalDataset(dataset: String, tables: List[String] = Nil)
