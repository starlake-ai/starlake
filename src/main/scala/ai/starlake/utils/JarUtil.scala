package ai.starlake.utils

import ai.starlake.job.Main

import java.io.{File, IOException}
import java.net.URISyntaxException
import java.util.jar.JarFile
import scala.collection.mutable.ListBuffer

// Utility object for handling JAR and resource file operations
object JarUtil {
  // Returns a list of resource file names under the given path, whether running from a JAR or IDE
  @throws[IOException]
  def getResourceFiles(path: String): List[String] = {
    val filenames = ListBuffer[String]()
    val jarFile = new File(
      classOf[Main].getProtectionDomain().getCodeSource().getLocation().getPath()
    )
    if (jarFile.isFile()) { // Run with JAR file
      val jar = new JarFile(jarFile)
      val entries = jar.entries // gives ALL entries in jar

      while (entries.hasMoreElements) {
        val entry = entries.nextElement()
        val name = entry.getName()
        if (!entry.isDirectory() && name.startsWith(path)) { // filter according to the path
          filenames.append(name)
        }
      }
      jar.close()
    } else { // Run with IDE
      val url = classOf[Main].getResource("/" + path)
      if (url != null) {
        val apps = new File(url.toURI)
        val pathWithSlash =
          if (path.endsWith("/"))
            path
          else
            path + "/"
        for (app <- apps.listFiles) {
          if (app.isDirectory) {
            // Recursively get files from subdirectories
            getResourceFiles(pathWithSlash + app.getName()).foreach(filenames.append(_))
          } else {
            filenames.append(pathWithSlash + app.getName())
          }
        }
      }
    }
    filenames.toList
  }

  // Returns a list of resource folder names under the given path, whether running from a JAR or IDE
  @throws[IOException]
  def getResourceFolders(path: String): List[String] = {
    val pathLevel = path.count(_ == '/')
    val pathLen = path.length
    val filenames = ListBuffer[String]()
    val jarFile = new File(
      classOf[Main].getProtectionDomain().getCodeSource().getLocation().getPath()
    )
    if (jarFile.isFile()) { // Run with JAR file
      val jar = new JarFile(jarFile)
      val entries = jar.entries // gives ALL entries in jar

      while (entries.hasMoreElements) {
        val entry = entries.nextElement()
        val name = entry.getName()
        val nameLevel = name.count(_ == '/')
        if (nameLevel == pathLevel + 1 && entry.isDirectory() && name.startsWith(path)) { // filter according to the path
          filenames.append(name.substring(pathLen, name.length - 1)) // remove trailing slash
        }
      }
      jar.close()
    } else { // Run with IDE
      val url = classOf[Main].getResource("/" + path)
      if (url != null) try {
        val apps = new File(url.toURI)
        for (app <- apps.listFiles) {
          filenames.append(app.getName())
        }
      } catch {
        case ex: URISyntaxException =>
        // never happens
      }
    }
    filenames.toList
  }

  // Checks if a resource name is a directory (used for testing)
  private def isDirectory(name: String): Boolean = {
    val resource = getContextClassLoader().getResource(name + "/")
    // this case for testing only
    if (resource.getProtocol.equals("file"))
      !name.contains(".")
    else
      resource != null
  }

  // Gets a resource as an InputStream, using the context class loader or fallback to getClass
  private def getResourceAsStream(resourceName: String) = {
    val in = getContextClassLoader().getResourceAsStream(resourceName)
    if (in == null) getClass.getResourceAsStream(resourceName)
    else in
  }

  // Returns the current thread's context class loader
  private def getContextClassLoader() = Thread.currentThread.getContextClassLoader()

  // Main method for testing resource folder listing
  def main(args: Array[String]): Unit = {
    // val files = getResourceFiles("bootstrap/samples/templates/any-source-any-sink")
    // files.foreach(println)
    val folders = getResourceFolders("bootstrap/samples/templates/")
    folders.foreach(println)
  }

}
