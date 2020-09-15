package com.ebiznext.comet.schema.model

/**
  * User / Group and Service accounts rights on a subset of the table.
  * @param name
  * @param predicate
  * @param grants
  */
case class RowLevelSecurity(
  name: String,
  predicate: String,
  grants: List[String]
) {

  def grantees(): List[(UserType, String)] = {
    grants.map { user =>
      val res = user.split(':')
      assert(res.length == 2)
      (UserType.fromString(res(0).trim), res(1).trim)
    }

  }
}

object RowLevelSecurity {

  def parse(input: String): RowLevelSecurity = {
    val components = input.split(',')
    assert(components.length >= 3)
    val name = components(0)
    val filter = components(1)
    val users = components.drop(2)
    RowLevelSecurity(name, filter, users.toList)
  }
}
