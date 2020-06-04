package com.ebiznext.comet.schema.model

case class RowLevelSecurity(
  name: String,
  predicate: String,
  grants: List[(UserType, String)]
)

object RowLevelSecurity {

  def parse(input: String): RowLevelSecurity = {
    val components = input.split(',')
    assert(components.length >= 3)
    val name = components(0)
    val filter = components(1)
    val users = components.drop(2)
    val grants = users.map { user =>
      val res = user.split(':')
      assert(res.length == 2)
      (UserType.fromString(res(0)), res(1))
    }
    RowLevelSecurity(name, filter, grants.toList)
  }
}
