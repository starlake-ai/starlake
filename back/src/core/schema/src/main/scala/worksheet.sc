import java.util.regex.Pattern

val res = Pattern.compile("a*b")
res.matcher("aaab").matches()


val res2 = Pattern.compile("SCHEMA-.*.dsv")
res2.matcher("SCHEMA-1.dsv").matches()