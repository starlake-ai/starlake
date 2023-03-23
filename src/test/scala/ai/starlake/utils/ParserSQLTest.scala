package ai.starlake.utils

import ai.starlake.TestHelper
import org.apache.spark.sql.catalyst.parser.ParserSQL

class ParserSQLTest extends TestHelper {
  new WithSettings() {
    "getTablesName" should "return a List of table name from sql query" in {
      val sqlWith =
        """ with
          |   alias0 as(select * from tableNon),
          |   ALIAS1 as (select * from table1),
          |   alias2 as (select * from alias1 left outer join table2 on table2.id = alias1.id),
          |   alias3 as (select * from alias2, alias1)
          | select * from alias2 as rename, alias3 left outer join table3 on table3.id = Alias2.id
          |""".stripMargin
      val sqlWith2 =
        """
          | with
          |   alias1 as (with
          |     alias2 as (select * from table1),
          |     alias3 as (select * from table2)
          |   select * from alias2, table3, alias2)
          | select * from alias1 left outer join (select * from table4)
          |""".stripMargin
      val sqlSubQuery =
        "select * from table1 as t1, (select * from table0) t0 left outer join table2 t2 on t2.id = t1.id"
      val sqlExtractExceptSafe =
        "select extract(dayofweek(monday) from last_day(date) ), * except(col), safe_cast(col as int64) as castField from `project.dataset.table`, dataset.table2"
      val sqlWithoutTable =
        """
          |select 'MONETIQUE_Externe'   as domain, 'APIF_MASTERCARD'            as schema, 'null'  as chef_file, 'oui' as lundi, 'oui' as mardi, 'oui' as mercredi, 'oui' as jeudi, 'oui' as vendredi, 'non' as samedi, 'non' as dimanche, 'non' as ferie, 'mensuel plusieurs fichiers manuel' as frequence, 'AB605010-AA_MM-DD-YYYY'                             as fichier union all
          |select 'MONETIQUE_Externe'   as domain, 'COUT_CPU'                   as schema, 'null'  as chef_file, 'oui' as lundi, 'oui' as mardi, 'oui' as mercredi, 'oui' as jeudi, 'oui' as vendredi, 'non' as samedi, 'non' as dimanche, 'non' as ferie, 'mensuel depot manuel'              as frequence, 'NXS-PROD-M-01-Optimisation des couts GT-YYYY-MM-DD' as fichier union all
          |select 'MONETIQUE_Externe'   as domain, 'EDIT_MASTERCARD'            as schema, 'null'  as chef_file, 'oui' as lundi, 'oui' as mardi, 'oui' as mercredi, 'oui' as jeudi, 'oui' as vendredi, 'non' as samedi, 'non' as dimanche, 'non' as ferie, 'mensuel depot manuel'              as frequence, 'Exported Full Month Top Error Report - M_D_YYYY'    as fichier
          |
          |
          |""".stripMargin
      val sqlUnnest =
        """
          |select * from table1, unnest(select col1 from table2), unnest(alias) where "val1" in unnest(["val1","val2"])
          |""".stripMargin

      ParserSQL.getTablesName(sqlWithoutTable) shouldEqual Nil
      ParserSQL.getTablesName(sqlWith) shouldEqual List("table1", "table2", "table3")
      ParserSQL.getTablesName(sqlWith2) shouldEqual List("table1", "table3", "table4")
      ParserSQL.getTablesName(sqlSubQuery) shouldEqual List("table1", "table0", "table2")
      ParserSQL.getTablesName(sqlExtractExceptSafe) shouldEqual List(
        "project.dataset.table",
        "dataset.table2"
      )
      ParserSQL.getTablesName(sqlUnnest) shouldEqual List("table1", "table2", "alias")

    }
  }
}
