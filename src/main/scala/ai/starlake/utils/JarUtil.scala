package ai.starlake.utils

import java.io.{File, IOException}
import java.net.{JarURLConnection, URISyntaxException, URL}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

// Utility object for handling JAR and resource file operations
object JarUtil {

  // Collects resource entries from a single classpath URL matching the given path
  private def collectFromUrl(
    url: URL,
    path: String,
    directoriesOnly: Boolean,
    filesOnly: Boolean,
    pathLevel: Option[Int] = None
  ): List[String] = {
    val filenames = ListBuffer[String]()
    val pathLen = path.length
    url.getProtocol match {
      case "jar" =>
        val connection = url.openConnection().asInstanceOf[JarURLConnection]
        val jar = connection.getJarFile
        try {
          val entries = jar.entries()
          while (entries.hasMoreElements) {
            val entry = entries.nextElement()
            val name = entry.getName
            if (name.startsWith(path)) {
              if (filesOnly && !entry.isDirectory) {
                filenames.append(name)
              } else if (directoriesOnly && entry.isDirectory) {
                pathLevel match {
                  case Some(level) =>
                    val nameLevel = name.count(_ == '/')
                    if (nameLevel == level + 1) {
                      filenames.append(name.substring(pathLen, name.length - 1))
                    }
                  case None =>
                    filenames.append(name.substring(pathLen, name.length - 1))
                }
              }
            }
          }
        } finally {
          jar.close()
        }

      case "file" =>
        try {
          val dir = new File(url.toURI)
          if (dir.isDirectory) {
            if (filesOnly) {
              listFilesRecursively(dir, path, filenames)
            } else if (directoriesOnly) {
              for (child <- Option(dir.listFiles()).getOrElse(Array.empty) if child.isDirectory) {
                filenames.append(child.getName)
              }
            }
          }
        } catch {
          case _: URISyntaxException => // ignore
        }

      case _ => // unsupported protocol, skip
    }
    filenames.toList
  }

  // Recursively lists files under a directory, building classpath-relative paths
  private def listFilesRecursively(
    dir: File,
    basePath: String,
    filenames: ListBuffer[String]
  ): Unit = {
    val pathWithSlash = if (basePath.endsWith("/")) basePath else basePath + "/"
    for (child <- Option(dir.listFiles()).getOrElse(Array.empty)) {
      if (child.isDirectory) {
        listFilesRecursively(child, pathWithSlash + child.getName, filenames)
      } else {
        filenames.append(pathWithSlash + child.getName)
      }
    }
  }

  // Returns a list of resource file names under the given path from all JARs/directories on the classpath
  @throws[IOException]
  def getResourceFiles(path: String): List[String] = {
    val classLoader = getContextClassLoader()
    val urls = classLoader.getResources(path).asScala.toList
    urls
      .flatMap(url => collectFromUrl(url, path, directoriesOnly = false, filesOnly = true))
      .distinct
  }

  // Returns a list of resource folder names under the given path from all JARs/directories on the classpath
  @throws[IOException]
  def getResourceFolders(path: String): List[String] = {
    val pathLevel = path.count(_ == '/')
    val classLoader = getContextClassLoader()
    val urls = classLoader.getResources(path).asScala.toList
    urls
      .flatMap(url =>
        collectFromUrl(
          url,
          path,
          directoriesOnly = true,
          filesOnly = false,
          pathLevel = Some(pathLevel)
        )
      )
      .distinct
  }

  // Returns the current thread's context class loader
  private def getContextClassLoader() = Thread.currentThread.getContextClassLoader

  // Main method for testing resource folder listing
  def main(args: Array[String]): Unit = {
    // val files = getResourceFiles("bootstrap/samples/templates/any-source-any-sink")
    // files.foreach(println)
    val folders = getResourceFolders("bootstrap/samples/templates/")
    folders.foreach(println)
  }

}
