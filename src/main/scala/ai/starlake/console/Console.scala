package ai.starlake.console

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Main
import ai.starlake.serve.{SingleUserMainServer, SingleUserServices}
import ai.starlake.utils.{JobResult, SparkJob}
import jline.console.history.FileHistory
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.config

import java.io.File
import scala.util.{Success, Try}

class Console(implicit isettings: Settings) {
  sealed trait Input
  case class InputLine(value: String) extends Input
  case object Blank extends Input
  case object EOF extends Input
  case object Previous extends Input
  case object Next extends Input

  val historyFile = new FileHistory(
    new File(System.getProperty("user.home") + File.separator + ".sl_history")
  )

  def isCommand(s: String): Boolean = {
    val userCmd = s.split(" ").headOption
    userCmd.exists(userCmd => Main.commands.exists(c => userCmd.startsWith(c.command)))
  }

  private var envValue: Option[String] = None

  def console(): Unit = {
    val consoleReader = new jline.console.ConsoleReader()
    consoleReader.setBellEnabled(false)
    consoleReader.setExpandEvents(false)
    consoleReader.setHistory(historyFile)

    var finished = false
    var currentInput = ""
    while (!finished) {
      val line = Option(consoleReader.readLine("> ")).map(_.trim).orNull
      currentInput += line
      if (line == null) {
        finished = handler(EOF)
      } else if (line.isEmpty) {
        finished = handler(Blank)
      } else if (line.startsWith("help")) {
        currentInput = ""
        finished = handler(InputLine(line))
      } else if (isCommand(line)) {
        currentInput = ""
        if (line.endsWith("?")) {
          new Main().printUsage(line.split(" ").head)
        } else
          finished = handler(InputLine(line))
      } else {
        finished = handler(InputLine(currentInput))
        currentInput = ""
      }
    }
    historyFile.flush()
  }

  private def handler(event: Input)(implicit isettings: Settings): Boolean = {
    val start = System.currentTimeMillis()
    val result =
      event match {
        case EOF =>
          println("CTRL-D")
          System.exit(0)
          true
        case Blank =>
          println("CTRL-D to quit")
          false

        case InputLine("help") =>
          new Main().printUsage()
          false
        case InputLine(s) if s.startsWith("help") =>
          new Main().printUsage(s.substring("help".length).trim)
          false
        case InputLine("q") | InputLine("quit") | InputLine("exit") =>
          System.exit(0)
          true
        case InputLine("reload") =>
          SingleUserServices.reset(reload = true)
          false
        case InputLine(s) if s.replaceAll("\\s+", "").startsWith("env=") =>
          val envValue = s.replaceAll("\\s+", "").substring("env=".length)
          this.envValue = if (envValue.isEmpty) None else Some(envValue)
          val exists =
            this.envValue
              .map(e => new Path(DatasetArea.metadata(isettings), s"env.$e.sl.yml"))
              .forall(isettings.storageHandler().exists)
          if (exists) {
            println(s"Setting env to $envValue")
            SingleUserServices.reset(reload = true)
          } else {
            println(s"Env $envValue does not exist")
          }
          false
        case InputLine(sqlDot) if sqlDot.startsWith(".") =>
          val sql = sqlDot.substring(1)
          new SparkJob {
            override def name: String = "console-job"
            override implicit def settings: Settings = isettings
            override def run(): Try[JobResult] = {
              Try {
                val df = session.sql(sql)
                if (!df.isEmpty)
                  df.show(false)
              } match {
                case scala.util.Success(_) =>
                  Success(JobResult.empty)
                case scala.util.Failure(e) =>
                  e.printStackTrace()
                  Success(JobResult.empty)
              }

            }
          }.run()
          false

        case InputLine(s) =>
          try {
            val params = s.split(" ")
            val response =
              SingleUserMainServer.run(
                isettings.appConfig.root,
                None,
                params,
                this.envValue,
                None,
                isettings.appConfig.duckdbMode
              )
            println(response)
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
          false

        case _ =>
          false
      }
    val end = System.currentTimeMillis()
    println(s"Time: ${end - start}ms")
    result
  }
}
