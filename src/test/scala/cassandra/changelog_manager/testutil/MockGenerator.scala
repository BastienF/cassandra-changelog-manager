package cassandra.changelog_manager.testutil

import java.io.File
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.cassandra.repository.SchemaVersionRepo
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.slf4j.{Logger => MockableLogger}

object MockGenerator extends MockitoSugar {

  def mockFile(fileName: String): File = {
    val file = mock[File]
    when(file.isFile).thenReturn(true)
    when(file.isDirectory).thenReturn(false)
    when(file.getName).thenReturn(fileName)
    file
  }

  def mockDir(dirName: String): File = {
    val dir = mock[File]
    when(dir.isFile).thenReturn(false)
    when(dir.isDirectory).thenReturn(true)
    when(dir.getName).thenReturn(dirName)
    dir
  }

  def mockContainingDir(files: List[File]): File = {
    val changelogDir = mock[File]
    when(changelogDir.listFiles()).thenReturn(files.toArray)
    changelogDir
  }

  def mockCassandraDatabase(schemaVersions: Set[SchemaVersion]): CassandraDatabase = {
    val cassandraDatabase = mock[CassandraDatabase]
    val schemaVersionRepo = mock[SchemaVersionRepo]
    when(cassandraDatabase.schemaVersionRepo).thenReturn(schemaVersionRepo)
    when(schemaVersionRepo.getAll(cassandraDatabase.appVersion)).thenReturn(schemaVersions)
    cassandraDatabase
  }

  def mockCassandraDatabase(sessions: Seq[Session], schemaVersionRepo: SchemaVersionRepo, appVersion: String = "v0"): CassandraDatabase = {
    val cassandraDatabase = mock[CassandraDatabase]
    when(schemaVersionRepo.getKeyspaceName).thenReturn("admin")
    when(cassandraDatabase.keyspacesSessions).thenReturn(sessions)
    when(cassandraDatabase.schemaVersionRepo).thenReturn(schemaVersionRepo)
    when(cassandraDatabase.appVersion).thenReturn(appVersion)
    cassandraDatabase
  }

  def mockSessionWithBoundStatement(cqlScript: String): (Session, BoundStatement) = {
    val session = mock[Session]
    val preparedStatement = mock[PreparedStatement]
    when(session.prepare(cqlScript)).thenReturn(preparedStatement)
    val boundStatement = mock[BoundStatement]
    when(preparedStatement.bind()).thenReturn(boundStatement)
    (session, boundStatement)
  }

  def mockLogger(): MockableLogger = {
    val logger = mock[MockableLogger]
    when(logger.isDebugEnabled).thenReturn(true)
    when(logger.isTraceEnabled).thenReturn(true)
    when(logger.isWarnEnabled).thenReturn(true)
    when(logger.isInfoEnabled).thenReturn(true)
    when(logger.isErrorEnabled).thenReturn(true)
    logger
  }
}
