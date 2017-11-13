package cassandra.changelog_manager.services


import java.io.{File, IOException}
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.exceptions.ReadTimeoutException
import com.typesafe.scalalogging.Logger
import org.slf4j.{Logger => MockableLogger}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import cassandra.changelog_manager.testutil.MockGenerator._
import scala.util.{Failure, Success, Try}

class ChangelogApplier_ApplyChangelogs_Test extends Specification with Mockito {

  "ChangelogApplier.applyChangelogs" should {
    class BasicMockedChangelogApplier extends ChangelogApplier {
      override def executeCQLStatements(cqlFile: File, cassandraDatabase: CassandraDatabase): Boolean = {
        true
      }

      override def notAlreadyApplied(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): List[File] = {
        cqlFiles
      }

      override def currentChangelogVersionDir(changelogDir: File): File = changelogDir
    }
    //GIVEN 4 ALL
    val schemaVersion = SchemaVersion("app_version", "script_name", "checksum", "executed_by", Date.from(Instant.now()), 10, "status")
    val cqlFile1 = mockFile("TestCQL1")
    val cqlFile2 = mockFile("TestCQL2")
    val changelogDir = mockContainingDir(List(cqlFile1, cqlFile2))
    val cassandraDatabase = mockCassandraDatabase(Set(schemaVersion))

    "success when all scripts are executables" in {
      class MockedChangelogApplier extends BasicMockedChangelogApplier
      //GIVEN
      val mockedChangelogApplier = spy(new MockedChangelogApplier)
      //WHEN
      val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

      //THEN
      result shouldEqual true
      there was one(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
      there was one(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
      there was one(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)
      success
    }

    "fail and stop execution after first cassandra error" in {
      class MockedChangelogApplier extends BasicMockedChangelogApplier {
        override def executeCQLStatements(cqlFile: File, cassandraDatabase: CassandraDatabase): Boolean = {
          throw new ReadTimeoutException(null, ConsistencyLevel.ANY, 10, 0, false)
        }
      }
      //GIVEN
      val mockedChangelogApplier = spy(new MockedChangelogApplier)
      //WHEN
      val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

      //THEN
      result shouldEqual false
      there was one(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
      there was one(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
      there was no(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)
      success
    }


    "execute no cql when all cql are already executed" in {
      class MockedChangelogApplier extends BasicMockedChangelogApplier {
        override def notAlreadyApplied(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): List[File] = {
          List()
        }
      }
      //GIVEN
      val mockedChangelogApplier = spy(new MockedChangelogApplier)
      //WHEN
      val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

      //THEN
      result shouldEqual true
      there was one(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
      there was no(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
      there was no(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)
      success
    }

    "execute only cql not already executed" in {
      class MockedChangelogApplier(notAlreadyApplied: File) extends BasicMockedChangelogApplier {
        override def notAlreadyApplied(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): List[File] = {
          List(notAlreadyApplied)
        }
      }
      //GIVEN
      val mockedChangelogApplier = spy(new MockedChangelogApplier(cqlFile2))
      //WHEN
      val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

      //THEN
      result shouldEqual true
      there was one(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
      there was no(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
      there was one(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)
      success
    }

    "fail and stop on IO Exception and if checksum differs" in {
      class MockedChangelogApplier(ioExceptionFile: File, checkSumDifferFile: File, mockedLogger: MockableLogger) extends BasicMockedChangelogApplier {
        override protected lazy val logger: Logger = Logger(mockedLogger)

        override def getAlteredExecutedScripts(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): Set[(String, Try[Boolean])] = {
          Set((ioExceptionFile.getName, Failure[Boolean](new IOException("Normal Error"))), (checkSumDifferFile.getName, Success(false)))
        }
      }
      //GIVEN
      val ioExceptionFile = mockFile("ioExceptionFile")
      val checkSumDifferFile = mockFile("checkSumDifferFile")
      val changelogDir = mockContainingDir(List(ioExceptionFile, checkSumDifferFile))
      val logger = mockLogger()
      val mockedChangelogApplier = spy(new MockedChangelogApplier(ioExceptionFile, checkSumDifferFile, logger))

      //WHEN
      val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

      //THEN
      result shouldEqual false
      there was one(mockedChangelogApplier).getAlteredExecutedScripts(List(ioExceptionFile, checkSumDifferFile), Set(schemaVersion))
      there was one(logger).error("checksum differs for: {}", "checkSumDifferFile")
      there was one(logger).error("IO error for ioExceptionFile:\njava.io.IOException: Normal Error")
      there was no(mockedChangelogApplier).executeCQLStatements(any(), any())
      success
    }
  }
}
