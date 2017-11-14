package cassandra.changelog_manager.services

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.cassandra.repository.SchemaVersionRepo
import cassandra.changelog_manager.testutil.MockGenerator._
import cassandra.changelog_manager.utils.CqlExecutionException
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.exceptions.ReadTimeoutException
import com.google.common.io.ByteSource
import com.typesafe.scalalogging.Logger
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import org.slf4j.{Logger => MockableLogger}

class ChangelogApplier_ExecuteCQLStatements_Test extends FlatSpec with MockitoSugar {

  class MockedChangelogApplier(mockedLogger: MockableLogger, mockedDate: Date) extends ChangelogApplier {

    case class StringByteSource(content: String) extends ByteSource {
      override def openStream(): InputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8.name))
    }

    override protected lazy val logger: Logger = Logger(mockedLogger)

    override def readFile(file: File): String = "SELECT * FROM sometable;"

    override def executeCQLStatements(cqlFile: File, cassandraDatabase: CassandraDatabase): Boolean = super.executeCQLStatements(cqlFile, cassandraDatabase)

    override protected def getFileAsByteSource(file: File): ByteSource = StringByteSource(file.toString)

    override protected def getCurrentDate: Date = mockedDate
  }

  "ChangelogApplier.executeCQLStatments" should "return true and execute some cql script on all sessions" in {
    //GIVEN
    val cqlFile = mockFile("TestCQL")
    val cqlScript = "SELECT * FROM sometable;"
    when(cqlFile.toString).thenReturn(cqlScript)

    val (session1, boundStatement1) = mockSessionWithBoundStatement(cqlScript)
    val (session2, boundStatement2) = mockSessionWithBoundStatement(cqlScript)
    val schemaVersionRepo = mock[SchemaVersionRepo]
    val cassandraDatabase = mockCassandraDatabase(Seq(session1, session2), schemaVersionRepo)

    val date = Date.from(Instant.now())
    val schemaVersion = SchemaVersion("v0", cqlFile.getName, "6120c242ff60bc066f7c53ed0403bd37", "", date, 0, "success")

    val logger = mockLogger()

    val mockedChangelogApplier = spy(new MockedChangelogApplier(logger, date))
    //WHEN
    val result = mockedChangelogApplier.executeCQLStatements(cqlFile, cassandraDatabase)

    //THEN
    assert(result)
    verify(mockedChangelogApplier).readFile(cqlFile)
    verify(session1).prepare(cqlScript)
    verify(session2).prepare(cqlScript)
    verify(session1).execute(boundStatement1)
    verify(session2).execute(boundStatement2)
    verify(schemaVersionRepo).insert(schemaVersion)
    verify(logger).info("{} executed with success", "TestCQL")
  }

  "ChangelogApplier.executeCQLStatments" should "return throw an error and stop on first error" in {

    //GIVEN
    val cqlFile = mockFile("TestCQL")
    val cqlScript = "SELECT * FROM sometable;"
    val exception = new ReadTimeoutException(null, ConsistencyLevel.ANY, 10, 0, false)

    val (session1, boundStatement1) = mockSessionWithBoundStatement(cqlScript)
    val (session2, boundStatement2) = mockSessionWithBoundStatement(cqlScript)
    val schemaVersionRepo = mock[SchemaVersionRepo]

    when(session1.execute(boundStatement1)).thenThrow(exception)
    when(session1.getLoggedKeyspace).thenReturn("keyspace1")

    val cassandraDatabase = mockCassandraDatabase(Seq(session1, session2), schemaVersionRepo)

    val logger = mockLogger()

    val mockedChangelogApplier = spy(new MockedChangelogApplier(logger, null))
    //WHEN
    assertThrows[CqlExecutionException] {
      mockedChangelogApplier.executeCQLStatements(cqlFile, cassandraDatabase)
    }

    //THEN
    verify(mockedChangelogApplier).readFile(cqlFile)
    verify(session1).prepare("SELECT * FROM sometable;")
    verify(session2, never()).prepare("SELECT * FROM sometable;")
    verify(session1).execute(boundStatement1)
    verify(session2, never()).execute(boundStatement2)
    verify(cassandraDatabase, never()).schemaVersionRepo
    verify(logger, never()).info("{} executed with success", "TestCQL")
    verify(logger).error("Failure on TestCQL:\n  failed query: SELECT * FROM sometable;\n  exception: com.datastax.driver.core.exceptions.ReadTimeoutException: Cassandra timeout during read query at consistency ANY (the replica queried for data didn't respond)"
    )
  }
}
