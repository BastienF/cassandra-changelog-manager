package cassandra.changelog_manager.services

import java.io.File
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import cassandra.changelog_manager.testutil.MockGenerator._

class ChangelogApplier_NotAlreadyApplied_Test extends Specification with Mockito {

  object MockedChangelogApplier extends ChangelogApplier {
    override def notAlreadyApplied(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): List[File] = super.notAlreadyApplied(cqlFiles, appliedVersions)
  }

  "ChangelogApplier.notAlreadyApplied" should {
    "return all files if no script has been applied" in {

      //GIVEN
      val cqlFile1 = mockFile("TestCQL")
      val cqlFile2 = mockFile("TestCQL2")
      val cqlFilesList = List(cqlFile1, cqlFile2)

      //WHEN
      val result = MockedChangelogApplier.notAlreadyApplied(cqlFilesList, Set())

      //THEN
      result shouldEqual cqlFilesList
      success
    }

    "return all not applied scripts" in {

      //GIVEN

      val schemaVersions = Set(generateSchemaVersions("already1"),generateSchemaVersions("already2"),generateSchemaVersions("already3"),generateSchemaVersions("already4"))


      val alreadyExecutedFiles = List(mockFile("already1"), mockFile("already2"), mockFile("already3"))
      val notExecutedFiles = List(mockFile("not1"), mockFile("not2"))

      val cqlFilesList: List[File] = alreadyExecutedFiles ++ notExecutedFiles

      //WHEN
      val result = MockedChangelogApplier.notAlreadyApplied(cqlFilesList, schemaVersions)

      //THEN
      result shouldEqual notExecutedFiles
      success
    }
  }

  private def generateSchemaVersions(fileName: String) = {
    SchemaVersion("app_version", fileName, "checksum", "executed_by", Date.from(Instant.now()), 10, "status")
  }
}
