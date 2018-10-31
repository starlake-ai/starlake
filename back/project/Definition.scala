import sbt.{ClasspathDependency, File, IntegrationTest, Project, file}

trait Definition {

  def play(name: String): Project = play(name, file(name))
  def play(name: String, src: File): Project =
    sbt
      .Project(id = name, base = src)

  /**
    * Creates a library module
    *
    * @param name the name of the module, will be used as the module's root folder name
    * @return the module's `Project`
    */
  def library(name: String): Project = library(name, file(name))

  /**
    * Creates a library sub project
    *
    * @param name the name of the project
    * @param src  the module's root folder
    * @return the module's `Project`
    */
  def library(name: String, src: File): Project =
    sbt
      .Project(id = name, base = src)
      .configs(IntegrationTest)

}
