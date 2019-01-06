import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.SchemaModel.Domain

System.getenv("HIVE_HOME")


val p1 = Pattern.compile("a*b")
p1.matcher("aaab").matches()


val p2 = Pattern.compile("SCHEMA-.*.dsv")
p2.matcher("SCHEMA-1.dsv").matches()


val name = "x.20190102-234415-936"
val p3 = Pattern.compile(".+\\.\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d-\\d\\d\\d")
p3.matcher(name).matches()

name.substring(0, name.lastIndexOf('.'))

Instant.now().toString
Instant.now().toString
Instant.now().toString
Instant.now().toString

LocalDateTime
  .now()
  .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))

LocalDateTime
  .now()
  .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))

LocalDateTime
  .now()
  .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))

LocalDateTime
  .now()
  .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))

LocalDateTime
  .now()
  .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))




val pdatetime = Pattern.compile("(19[0-9]{2}|2[0-9]{3})-(0[1-9]|1[012])-([123]0|[012][1-9]|31) ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])")
pdatetime.matcher("2019-12-24 23:54:44").matches()


val pdate = Pattern.compile("(19[0-9]{2}|2[0-9]{3})-(0[1-9]|1[012])-([123]0|[012][1-9]|31)")
pdate.matcher("2019-12-24").matches()

val pdouble = Pattern.compile("([0-9][0-9]*)(\\.*)[0-9]*")
pdouble.matcher("12.23").matches()

val pint = Pattern.compile("[0-9][0-9]*")
pint.matcher("1223").matches()

val pdate2 = Pattern.compile("(19[0-9]{2}|2[0-9]{3})-(0[1-9]|1[012])-([123]0|[012][1-9]|31)( ([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]))?")
pdate2.matcher("2018-12-24 12:23:56").matches()

