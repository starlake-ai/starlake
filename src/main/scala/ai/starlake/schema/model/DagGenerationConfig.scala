package ai.starlake.schema.model

case class DagGenerationConfig(
  schedule: Option[String],
  cluster: Option[ClusterConfig],
  sl: Option[SlConfig],
  dagName: Option[String],
  suffixWithIndex: Option[Boolean]
)

case class ClusterConfig(profileVar: Option[String], region: Option[String])

/** @param envVar
  * @param jarFileUrisVar
  *   ["gs://.../starlake
  */
case class SlConfig(envVar: Option[String], jarFileUrisVar: Option[String])

object DagGenerationConfig {

  val defaultDagNamePattern = "{{domain}}"

  val defaultSuffixWithIndex = true
  implicit def dagGenerationConfigMerger(
    parent: DagGenerationConfig,
    child: DagGenerationConfig
  ): DagGenerationConfig = {
    DagGenerationConfig(
      schedule = child.schedule.orElse(parent.schedule),
      cluster = child.cluster.orElse(parent.cluster),
      sl = child.sl
        .map(childConfig => {
          parent.sl match {
            case Some(parentConfig) =>
              SlConfig(
                envVar = childConfig.envVar.orElse(parentConfig.envVar),
                jarFileUrisVar = childConfig.jarFileUrisVar.orElse(parentConfig.jarFileUrisVar)
              )
            case None => childConfig
          }
        })
        .orElse(parent.sl),
      dagName = child.dagName.orElse(parent.dagName),
      suffixWithIndex = child.suffixWithIndex.orElse(parent.suffixWithIndex)
    )
  }
}
