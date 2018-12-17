import java.util.regex.Pattern

System.getenv("HIVE_HOME")


val p1 = Pattern.compile("a*b")
p1.matcher("aaab").matches()


val p2 = Pattern.compile("SCHEMA-.*.dsv")
p2.matcher("SCHEMA-1.dsv").matches()


val p3 = Pattern.compile(".+")
p3.matcher("x").matches()
