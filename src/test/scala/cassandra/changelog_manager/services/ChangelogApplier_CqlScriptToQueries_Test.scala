package cassandra.changelog_manager.services


import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

class ChangelogApplier_CqlScriptToQueries_Test extends FlatSpec with MockitoSugar {

  class MockedChangelogApplier extends ChangelogApplier {
    override def cqlScriptToQueries(script: String): List[String] = super.cqlScriptToQueries(script)
  }


  val splitComment = """-- cassandra-changelog-manager statement"""
  "ChangelogApplier.cqlScriptToQueries" should "return one query if there is only one" in {
    //GIVEN
    val query = "SELECT * FROM table;"
    val cqlScript =
      s"""$splitComment
         |$query""".stripMargin

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.cqlScriptToQueries(cqlScript)

    //THEN
    assert(result.equals(List(query)))
  }

  "ChangelogApplier.cqlScriptToQueries" should "return multiple queries" in {
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
    assert(result.equals(List(query, query2)))
  }

  "ChangelogApplier.cqlScriptToQueries" should "return multiple queries when trailling white spaces" in {
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
    assert(result.equals(List(query, query2)))
  }

  "ChangelogApplier.cqlScriptToQueries" should "return multiple queries when empty lines" in {
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
    assert(result.equals(List(query, query2)))
  }
}
