package com.ebiznext.comet.sample

import java.io.InputStream
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.SchemaModel._

trait SampleData {
  /**
    * types:
    * - name: "string"
    * primitiveType: "string"
    * pattern: ".+"
    * - name: "time"
    * primitiveType: "string"
    * pattern: "(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)"
    * - name: "time24"
    * primitiveType: "string"
    * pattern: "([01]?[0-9]|2[0-3]):[0-5][0-9]"
    * - name: "date"
    * primitiveType: "date"
    * pattern: "(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)"
    * - name: "username"
    * primitiveType: "string"
    * pattern: "[a-z0-9_-]{3,15}"
    * - name: "age"
    * primitiveType: "long"
    * pattern: "[0-9]{1,15}"
    * - name: "color"
    * primitiveType: "string"
    * pattern: "#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})"
    * - name: "ip"
    * primitiveType: "string"
    * pattern: "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])"
    * - name: "email"
    * primitiveType: "string"
    * pattern: "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}"
    */

    def loadFile(filename:String): String = {
      val stream: InputStream = getClass.getResourceAsStream(filename)
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    }

  val types = Types(
    List(
      Type("string", Pattern.compile(".+"), PrimitiveType.string),
      Type("time", Pattern.compile("(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)")),
      Type("time24", Pattern.compile("([01]?[0-9]|2[0-3]):[0-5][0-9]")),
      Type("data", Pattern.compile("(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)"), PrimitiveType.date),
      Type("username", Pattern.compile("[a-z0-9_-]{3,15}")),
      Type("age", Pattern.compile("[a-z0-9_-]{3,15}"), PrimitiveType.long),
      Type("color", Pattern.compile("#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})"), PrimitiveType.string),
      Type("ip", Pattern.compile("([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])")),
      Type("email", Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}"))
    )
  )

  val domain = Domain("DOMAIN", "/tmp/incoming/DOMAIN",
    Metadata(
      Some(Mode.FILE),
      Some(Format.DSV),
      Some(false),
      Some(";"),
      Some("\""),
      Some("\\"),
      Some(Write.APPEND),
      None,
      Some("yyyy-MM-dd"),
      Some("yyyy-MM-dd HH:mm:ss")),
    List(
      Schema("User", Pattern.compile("SCHEMA-.*.dsv"),
        List(
          DSVAttribute("firstname", "string", false, PrivacyLevel.NONE),
          DSVAttribute("lastname", "string", false, PrivacyLevel.SHA1),
          DSVAttribute("age", "age", false, PrivacyLevel.HIDE)
        ),
        Some(Metadata(withHeader = Some(true)))
      )
    )
  )
}
