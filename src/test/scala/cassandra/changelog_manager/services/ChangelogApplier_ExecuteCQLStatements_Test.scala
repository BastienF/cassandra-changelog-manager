package cassandra.changelog_manager.services

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.cassandra.repository.SchemaVersionRepo
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.exceptions.ReadTimeoutException
import com.google.common.io.ByteSource
import com.typesafe.scalalogging.Logger
import org.slf4j.{Logger => MockableLogger}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import cassandra.changelog_manager.testutil.MockGenerator._
import cassandra.changelog_manager.utils.CqlExecutionException

class ChangelogApplier_ExecuteCQLStatements_Test extends Specification with Mockito {

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

  "ChangelogApplier.executeCQLStatments" should {
    "return true and execute some cql script on all sessions" in {
      //GIVEN
      val cqlFile = mockFile("TestCQL")
      val cqlScript = "SELECT * FROM sometable;"
      cqlFile.toString returns cqlScript

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
      result shouldEqual true
      there was one(mockedChangelogApplier).readFile(cqlFile)
      there was one(session1).prepare(cqlScript)
      there was one(session2).prepare(cqlScript)
      there was one(session1).execute(boundStatement1)
      there was one(session2).execute(boundStatement2)
      there was one(schemaVersionRepo).insert(schemaVersion)
      there was one(logger).info("{} executed with success", "TestCQL")

      success
    }

    "return throw an error and stop on first error" in {

      //GIVEN
      val cqlFile = mockFile("TestCQL")
      val cqlScript = "SELECT * FROM sometable;"
      val exception = new ReadTimeoutException(null, ConsistencyLevel.ANY, 10, 0, false)

      val (session1, boundStatement1) = mockSessionWithBoundStatement(cqlScript)
      val (session2, boundStatement2) = mockSessionWithBoundStatement(cqlScript)
      val schemaVersionRepo = mock[SchemaVersionRepo]

      session1.execute(boundStatement1) throws exception
      session1.getLoggedKeyspace returns "keyspace1"

      val cassandraDatabase = mockCassandraDatabase(Seq(session1, session2), schemaVersionRepo)

      val logger = mockLogger()

      val mockedChangelogApplier = spy(new MockedChangelogApplier(logger, null))
      //WHEN
      mockedChangelogApplier.executeCQLStatements(cqlFile, cassandraDatabase) must throwA(CqlExecutionException("TestCQL", "keyspace1", exception))

      //THEN
      there was one(mockedChangelogApplier).readFile(cqlFile)
      there was one(session1).prepare("SELECT * FROM sometable;")
      there was no(session2).prepare("SELECT * FROM sometable;")
      there was one(session1).execute(boundStatement1)
      there was no(session2).execute(boundStatement2)
      there was no(cassandraDatabase).schemaVersionRepo
      there was no(logger).info("{} executed with success", "TestCQL")
      there was one(logger).error("Failure on TestCQL:\n  failed query: SELECT * FROM sometable;\n  exception: com.datastax.driver.core.exceptions.ReadTimeoutException: Cassandra timeout during read query at consistency ANY (the replica queried for data didn't respond)"
      )

      success
    }

  }
}
