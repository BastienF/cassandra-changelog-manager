package cassandra.changelog_manager.services

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

class ChangelogApplier_CqlScriptToQueries_Test extends Specification with Mockito {

  class MockedChangelogApplier extends ChangelogApplier {
    override def cqlScriptToQueries(script: String): List[String] = super.cqlScriptToQueries(script)
  }

  "ChangelogApplier.cqlScriptToQueries" should {
    val splitComment = """-- cassandra-changelog-manager statement"""
    "return one query if there is only one" in {
      //GIVEN
      val query = "SELECT * FROM table;"
      val cqlScript =
        s"""$splitComment
           |$query""".stripMargin

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.cqlScriptToQueries(cqlScript)

      //THEN
      result shouldEqual List(query)
      success
    }

    "return multiple queries" in {
      //GIVEN
      val query = "SELECT * FROM table;"
      val query2 = "SELECT * FROM table2;"
      val cqlScript =
        s"""$splitComment
           |$query
           |$splitComment
           |$query2
         """.stripMargin

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.cqlScriptToQueries(cqlScript)

      //THEN
      result shouldEqual List(query, query2)
      success
    }

    "return multiple queries when trailling white spaces" in {
      //GIVEN
      val query = "SELECT * FROM table;"
      val query2 = "SELECT * FROM table2;"
      val cqlScript =
        s"""$splitComment
           |$query
           |$splitComment
           |$query2
         """.stripMargin

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.cqlScriptToQueries(cqlScript)

      //THEN
      result shouldEqual List(query, query2)
      success
    }

    "return multiple queries when empty lines" in {
      //GIVEN
      val query = "SELECT * FROM table;"
      val query2 = "SELECT * FROM table2;"
      val cqlScript =
        s"""
           |
           |$splitComment
           |
           |$query
           |
           |$splitComment
           |
           |
           |$query2
           |
         """.stripMargin

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.cqlScriptToQueries(cqlScript)

      //THEN
      result shouldEqual List(query, query2)
      success
    }
  }
}
