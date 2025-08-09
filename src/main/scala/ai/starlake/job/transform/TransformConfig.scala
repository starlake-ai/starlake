package ai.starlake.job.transform

case class TransformConfig(
  name: String = "",
  options: Map[String, String] = Map.empty,
  compile: Boolean = false,
  syncApply: Boolean = false,
  syncPreview: Boolean = false,
  tags: Seq[String] = Seq.empty,
  format: Boolean = false,
  interactive: Option[String] = None,
  reload: Boolean = false,
  truncate: Boolean = false,
  recursive: Boolean = false,
  test: Boolean = false,
  accessToken: Option[String] = None,
  dryRun: Boolean = false,
  query: Option[String] = None,
  pageSize: Int = 1000,
  pageNumber: Int = 1,
  scheduledDate: Option[String]
) {
  def optionsAsString: String = options.map { case (k, v) => s"$k=$v" }.mkString(",")
  override def toString: String = {
    s"""
       |name=$name
       |options=$optionsAsString
       |compile=$compile
       |syncApply=$syncApply
       |syncPreview=$syncPreview
       |tags=$tags
       |format=$format
       |interactive=$interactive
       |reload=$reload
       |truncate=$truncate
       |recursive=$recursive
       |test=$test
       |accessToken=$accessToken
       |dryRun=$dryRun
       |query=$query
       |pageSize=$pageSize
       |pageNumber=$pageNumber
       |""".stripMargin
  }
}
