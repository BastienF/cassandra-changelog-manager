package cassandra.changelog_manager.testutil

import java.io.File
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.cassandra.repository.SchemaVersionRepo
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}
import org.specs2.mock.Mockito
import org.slf4j.{Logger => MockableLogger}

object MockGenerator extends Mockito {

  def mockFile(fileName: String): File = {
    val file = mock[File]
    file.isFile returns true
    file.isDirectory returns false
    file.getName returns fileName
    file
  }

  def mockDir(dirName: String): File = {
    val dir = mock[File]
    dir.isFile returns false
    dir.isDirectory returns true
    dir.getName returns dirName
    dir
  }

  def mockContainingDir(files: List[File]): File = {
    val changelogDir = mock[File]
    changelogDir.listFiles() returns files.toArray
    changelogDir
  }

  def mockCassandraDatabase(schemaVersions: Set[SchemaVersion]): CassandraDatabase = {
    val cassandraDatabase = mock[CassandraDatabase]
    val schemaVersionRepo = mock[SchemaVersionRepo]
    cassandraDatabase.schemaVersionRepo returns schemaVersionRepo
    schemaVersionRepo.getAll returns schemaVersions
    cassandraDatabase
  }

  def mockCassandraDatabase(sessions: Seq[Session], schemaVersionRepo: SchemaVersionRepo): CassandraDatabase = {
    val cassandraDatabase = mock[CassandraDatabase]
    schemaVersionRepo.getKeyspaceName returns "admin"
    cassandraDatabase.keyspacesSessions returns sessions
    cassandraDatabase.schemaVersionRepo returns schemaVersionRepo
    cassandraDatabase
  }

  def mockSessionWithBoundStatement(cqlScript: String): (Session, BoundStatement) = {
    val session = mock[Session]
    val preparedStatement = mock[PreparedStatement]
    session.prepare(cqlScript) returns preparedStatement
    val boundStatement = mock[BoundStatement]
    preparedStatement.bind() returns boundStatement
    (session, boundStatement)
  }

  def mockLogger(): MockableLogger = {
    val logger = mock[MockableLogger]
    logger.isDebugEnabled returns true
    logger.isTraceEnabled returns true
    logger.isWarnEnabled returns true
    logger.isInfoEnabled returns true
    logger.isErrorEnabled returns true
    logger
  }

}
