package ai.starlake.utils

import ai.starlake.TestHelper

import java.util.regex.Pattern

class CompilerUtilsSpec extends TestHelper {
  "compileWriteStrategy" should "validate strategy code" in {
    val matcher = Pattern.compile("(.*)\\.(.*)$").matcher("my-file.txt")
    matcher.find()
    val context = Map(
      "matcher"               -> matcher,
      "fileSize"              -> 10L,
      "isFirstDayOfMonth"     -> true,
      "isLastDayOfMonth"      -> false,
      "dayOfWeek"             -> 2,
      "isFileFirstDayOfMonth" -> false,
      "isFileLastDayOfMonth"  -> true,
      "fileDayOfWeek"         -> 3
    )
    CompilerUtils.compileWriteStrategy(""" group(1) == "my-file" """)(context) should be(
      true
    )
    CompilerUtils.compileWriteStrategy(""" group(1) == "other-file" """)(
      context
    ) should be(false)
    CompilerUtils.compileWriteStrategy(""" group(2) == "txt" """)(context) should be(
      true
    )
    CompilerUtils.compileWriteStrategy(
      """ group(1) == "my-file" && group(2) == "txt" """
    )(context) should be(true)
    CompilerUtils.compileWriteStrategy(""" fileSizeB == 10 && group(2) == "txt" """)(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(""" fileSizeB >= 10 && group(2) == "txt" """)(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(""" fileSizeB <= 10 && group(2) == "txt" """)(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(""" fileSizeB < 10 && group(2) == "txt" """)(
      context
    ) should be(false)
    CompilerUtils.compileWriteStrategy(""" fileSizeKo == 0 && group(2) == "txt" """)(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(
      """ isFirstDayOfMonth && dayOfWeek == 2 && !isLastDayOfMonth """
    )(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(
      """ !isFileFirstDayOfMonth && fileDayOfWeek == 3 && isFileLastDayOfMonth """
    )(
      context
    ) should be(true)
  }

  "compileWriteStrategy" should "compile with regexp without capture groups" in {
    val matcher = Pattern.compile("my-regex").matcher("my-file.txt")
    matcher.find()
    val context = Map(
      "matcher"               -> matcher,
      "fileSize"              -> 10L,
      "isFirstDayOfMonth"     -> true,
      "isLastDayOfMonth"      -> false,
      "dayOfWeek"             -> 2,
      "isFileFirstDayOfMonth" -> false,
      "isFileLastDayOfMonth"  -> true,
      "fileDayOfWeek"         -> 2
    )
    CompilerUtils.compileWriteStrategy(""" fileSizeB == 10 """)(
      context
    ) should be(true)
  }

  "compileWriteStrategy" should "compile with regexp with named capture groups" in {
    val matcher = Pattern.compile(".*-(?<mode>FULL|APPEND).csv$").matcher("my-file-FULL.csv")
    matcher.find()
    val context = Map(
      "matcher"               -> matcher,
      "fileSize"              -> 10L,
      "isFirstDayOfMonth"     -> true,
      "isLastDayOfMonth"      -> false,
      "dayOfWeek"             -> 2,
      "isFileFirstDayOfMonth" -> false,
      "isFileLastDayOfMonth"  -> true,
      "fileDayOfWeek"         -> 2
    )
    CompilerUtils.compileWriteStrategy(""" group("mode") == "FULL" """)(
      context
    ) should be(true)
    CompilerUtils.compileWriteStrategy(""" group("mode") == "APPEND" """)(
      context
    ) should be(false)
  }

  "compileExpectation" should "validate strategy code" in {
    val context =
      Map("count" -> 1L, "result" -> List(1, 2, 3), "results" -> List(List(1, 2), List(3, 4)))
    CompilerUtils.compileExpectation("count == 1L")(context) should be(true)
    CompilerUtils.compileExpectation("count == 2L")(context) should be(false)
    CompilerUtils.compileExpectation("count == 1L && result.contains(3)")(context) should be(true)
    CompilerUtils.compileExpectation("count == 1L && result.contains(3) && results.size == 2")(
      context
    ) should be(true)
  }
}
