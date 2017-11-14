package cassandra.changelog_manager.services

import java.io.{File, IOException}
import java.time.Instant
import java.util.Date

import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.testutil.MockGenerator._
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.exceptions.ReadTimeoutException
import com.typesafe.scalalogging.Logger
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import org.slf4j.{Logger => MockableLogger}

import scala.util.{Failure, Success, Try}

class ChangelogApplier_ApplyChangelogs_Test extends FlatSpec with MockitoSugar {

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
  private val schemaVersion = SchemaVersion("app_version", "script_name", "checksum", "executed_by", Date.from(Instant.now()), 10, "status")
  private val cqlFile1 = mockFile("TestCQL1")
  private val cqlFile2 = mockFile("TestCQL2")
  private val changelogDir = mockContainingDir(List(cqlFile1, cqlFile2))
  private val cassandraDatabase = mockCassandraDatabase(Set(schemaVersion))

  "ChangelogApplier.applyChangelogs" should "success when all scripts are executables" in {
    class MockedChangelogApplier extends BasicMockedChangelogApplier
    //GIVEN

    val mockedChangelogApplier = spy(new MockedChangelogApplier)
    //WHEN
    val result = mockedChangelogApplier.applyChangelogs(cassandraDatabase, changelogDir)

    //THEN
    assert(result)
    verify(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
    verify(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
    verify(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)

  }

  "ChangelogApplier.applyChangelogs" should "fail and stop execution after first cassandra error" in {
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
    assert(!result)
    verify(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
    verify(mockedChangelogApplier).executeCQLStatements(cqlFile1, cassandraDatabase)
    verify(mockedChangelogApplier, never()).executeCQLStatements(cqlFile2, cassandraDatabase)
  }


  "ChangelogApplier.applyChangelogs" should "execute no cql when all cql are already executed" in {
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
    assert(result)
    verify(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
    verify(mockedChangelogApplier, never()).executeCQLStatements(cqlFile1, cassandraDatabase)
    verify(mockedChangelogApplier, never()).executeCQLStatements(cqlFile2, cassandraDatabase)
  }

  "ChangelogApplier.applyChangelogs" should "execute only cql not already executed" in {
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
    assert(result)
    verify(mockedChangelogApplier).notAlreadyApplied(List(cqlFile1, cqlFile2), Set(schemaVersion))
    verify(mockedChangelogApplier, never()).executeCQLStatements(cqlFile1, cassandraDatabase)
    verify(mockedChangelogApplier).executeCQLStatements(cqlFile2, cassandraDatabase)
  }

  "ChangelogApplier.applyChangelogs" should "fail and stop on IO Exception and if checksum differs" in {
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
    assert(!result)
    verify(mockedChangelogApplier).getAlteredExecutedScripts(List(ioExceptionFile, checkSumDifferFile), Set(schemaVersion))
    verify(logger).error("checksum differs for: {}", "checkSumDifferFile")
    verify(logger).error("IO error for ioExceptionFile:\njava.io.IOException: Normal Error")
    verify(mockedChangelogApplier, never()).executeCQLStatements(any(), any())
  }
}
