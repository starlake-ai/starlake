package ai.starlake.repl

import jline.console.ConsoleReader
import jline.console.completer.StringsCompleter

import java.io.{IOException, PrintWriter}

object Main {
  def usage(): Unit = {
    System.out.println(
      "Usage: java Example [none/simple/files/dictionary [trigger mask]]"
    )
    System.out.println("  none - no completors")
    System.out.println(
      "  simple - a simple completor that comples " + "\"foo\", \"bar\", and \"baz\""
    )
    System.out.println("  files - a completor that comples " + "file names")
    System.out.println("  classes - a completor that comples " + "java class names")
    System.out.println(
      "  trigger - a special word which causes it to assume " + "the next line is a password"
    )
    System.out.println(
      "  mask - is the character to print in place of " + "the actual password character"
    )
    System.out.println("  color - colored prompt and feedback")
    System.out.println(
      "\n  E.g - java Example simple su '*'\n" + "will use the simple compleator with 'su' triggering\n" + "the use of '*' as a password mask."
    )
  }

  @throws[IOException]
  def main(args: Array[String]): Unit = {
    try {
      var mask: Character = null
      var trigger: String = null
      var color: Boolean = false
      val reader = new ConsoleReader()
      reader.setPrompt("prompt> ")
      /*      val completors = new util.LinkedList[Completer]
      if (args.length > 0)
        if (args(0) == "none") {} else if (args(0) == "files")
          completors.add(new FileNameCompleter())
        else if (args(0) == "simple")
          completors.add(new StringsCompleter("foo", "bar", "baz"))
        else if (args(0) == "color") {
          color = true
          reader.setPrompt("\u001B[42mfoo\u001B[0m@bar\u001B[32m@baz\u001B[0m> ")
          val handler = new CandidateListCompletionHandler
          reader.setCompletionHandler(handler)
        } else {
          usage()
          return

        }

       */
      if (args.length == 3) {
        mask = args(2).charAt(0)
        trigger = args(1)
      }
      reader.addCompleter(new StringsCompleter("foo", "bar", "baz"))
      var line = reader.readLine()
      val out = new PrintWriter(reader.getOutput)
      while (line != null) {
        if (color)
          out.println("\u001B[33m======>\u001B[0m\"" + line + "\"")
        else
          out.println("======>\"" + line + "\"")
        out.flush()
        // If we input the special word then we will mask
        // the next line.
        if ((trigger != null) && (line.compareTo(trigger) == 0))
          line = reader.readLine("password> ", mask)
        if (line.equalsIgnoreCase("cls")) reader.clearScreen
        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit"))
          System.exit(1)
        line = reader.readLine()
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }
}
