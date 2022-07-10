package ai.starlake.schema.model

/** User / Group and Service accounts rights on a subset of the table.
  * @param name
  *   : This Row Level Security unique name
  * @param predicate
  *   : The condition that goes to the WHERE clause and limitt the visible rows.
  * @param grants
  *   : user / groups / service accounts to which this security level is applied. ex :
  *   user:me@mycompany.com,group:group@mycompany.com,serviceAccount:mysa@google-accounts.com
  */
case class RowLevelSecurity(
  name: String,
  predicate: String = "TRUE",
  grants: Set[String],
  description: String = ""
) {

  def grantees(): Set[(UserType, String)] = {
    grants.map { user =>
      val res = user.split(':')
      assert(res.length == 2)
      (UserType.fromString(res(0).trim), res(1).trim)
    }
  }
}

object RowLevelSecurity {

  def parse(input: String): RowLevelSecurity = {
    val components = input.split('/')
    assert(components.length >= 3)
    val name = components(0)
    val filter = components(1)
    val users = components.drop(2)
    RowLevelSecurity(name, filter, users.toSet)
  }
}
