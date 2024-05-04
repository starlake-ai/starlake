package ai.starlake.console

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.serve.SingleUserMainServer
import jline.console.history.FileHistory

import java.io.File

object Console {
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

  def console(handler: Input => Boolean): Unit = {

    val consoleReader = new jline.console.ConsoleReader()
    consoleReader.setBellEnabled(false)
    consoleReader.setExpandEvents(false)
    consoleReader.setHistory(historyFile)

    var finished = false
    var currentInput = ""
    while (!finished) {
      val line = consoleReader.readLine("> ")
      currentInput += line + "\n"
      if (line == null) {
        finished = handler(EOF)
      } else if (line.isEmpty) {
        finished = handler(Blank)
      } else if (line.startsWith("help")) {
        finished = handler(InputLine(line))
      } else if (isCommand(line)) {
        finished = handler(InputLine(line))
      } else if (line.trim().endsWith(";")) {
        finished = handler(InputLine(currentInput))
        currentInput = ""
      }
    }
    historyFile.flush()
  }

  def handler(event: Input)(implicit isettings: Settings): Boolean = {
    event match {
      case EOF =>
        println("CTRL-D")
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
      case InputLine("q") =>
        true
      case InputLine(s) =>
        try {
          val params = s.split(" ")
          val response =
            SingleUserMainServer.run(isettings.appConfig.root, None, params, None, None)
          println(response)
        } catch {
          case e: Throwable =>
            e.printStackTrace()
        }
        false
      /*
      case InputLine(sql) if 1 == 2 =>
        new SparkJob {
          override def name: String = "console-job"
          override implicit def settings: config.Settings = isettings
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

       */
      case _ =>
        false
    }
  }
}
