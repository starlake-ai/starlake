package ai.starlake.utils

import java.io.{BufferedReader, IOException, InputStreamReader}
import scala.collection.mutable.ListBuffer

object JarUtil {
  @throws[IOException]
  def getResourceFiles(path: String): List[String] = {
    val filenames = ListBuffer[String]()
    val in = getResourceAsStream(path)
    val br = new BufferedReader(new InputStreamReader(in))
    try {
      var resource: String = br.readLine()
      while (resource != null) {
        val resPath = s"$path/$resource"
        if (isDirectory(resPath)) {
          getResourceFiles(s"$resPath").foreach(it => filenames.append(it))
        } else {
          filenames.append(resPath)
        }
        resource = br.readLine()
      }
    } finally {
      if (in != null) in.close()
      if (br != null) br.close()
    }
    filenames.toList
  }

  @throws[IOException]
  def getResourceFolders(path: String): List[String] = {
    val filenames = ListBuffer[String]()
    val in = getResourceAsStream(path)
    val br = new BufferedReader(new InputStreamReader(in))
    try {
      var resource: String = br.readLine()
      while (resource != null) {
        val resPath = s"$path/$resource"
        if (isDirectory(resPath)) {
          filenames.append(resource)
        }
        resource = br.readLine()
      }
    } finally {
      if (in != null) in.close()
      if (br != null) br.close()
    }
    filenames.toList
  }

  private def isDirectory(name: String): Boolean = {
    val resource = getContextClassLoader().getResource(name + "/")
    // this case for testing only
    if (resource.getProtocol.equals("file"))
      !name.contains(".")
    else
      resource != null
  }

  private def getResourceAsStream(resourceName: String) = {
    val in = getContextClassLoader().getResourceAsStream(resourceName)
    if (in == null) getClass.getResourceAsStream(resourceName)
    else in
  }

  private def getContextClassLoader() = Thread.currentThread.getContextClassLoader()

  def main(args: Array[String]): Unit = {
    val files = getResourceFiles("bootstrap/samples/templates/any-source-any-sink")
    files.foreach(println)
    val folders = getResourceFolders("bootstrap/samples/templates")
    folders.foreach(println)
  }
}
