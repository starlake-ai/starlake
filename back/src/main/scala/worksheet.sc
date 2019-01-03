import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}
import java.util.regex.Pattern

System.getenv("HIVE_HOME")


val p1 = Pattern.compile("a*b")
p1.matcher("aaab").matches()


val p2 = Pattern.compile("SCHEMA-.*.dsv")
p2.matcher("SCHEMA-1.dsv").matches()


val name ="x.20190102-234415-936"
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
