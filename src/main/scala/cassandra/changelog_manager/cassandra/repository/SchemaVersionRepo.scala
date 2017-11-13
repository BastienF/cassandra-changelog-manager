package cassandra.changelog_manager.cassandra.repository

import cassandra.changelog_manager.cassandra.models.SchemaVersion
import com.datastax.driver.core.{ResultSet, Row, Session}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class SchemaVersionRepo(session: Session) {

  private final lazy val getAllpreparedStatement = session.prepare(s"SELECT * FROM schema_version;")
  private final lazy val insertStatement = session.prepare(s"INSERT INTO schema_version (app_version, script_name, checksum, executed_by, executed_on, execution_time, status) VALUES (?, ?, ?, ?, ?, ?, ?);")
  private final lazy val initTableStatement = session.prepare(
    """CREATE TABLE IF NOT EXISTS schema_version (
      script_name text,
      app_version text,
      checksum text,
      executed_by text,
      executed_on timestamp,
      execution_time int,
      status text,
      PRIMARY KEY(app_version, script_name));""")

  private def schemaVersion(row: Row): SchemaVersion =
  SchemaVersion(row.getString("app_version"),
  row.getString("script_name"),
  row.getString("checksum"),
  row.getString("executed_by"),
  row.getTimestamp("executed_on"),
  row.getInt("execution_time"),
  row.getString("status"),
  )

  def getAll: Set[SchemaVersion] = {
    session.execute(getAllpreparedStatement.bind()).all().asScala.map(schemaVersion).toSet
  }

  def insert(schemaVersion: SchemaVersion): ResultSet = {
    session.execute(insertStatement.bind(schemaVersion.app_version, schemaVersion.script_name, schemaVersion.checksum, schemaVersion.executed_by, schemaVersion.executed_on, int2Integer(schemaVersion.execution_time), schemaVersion.status))
  }

  def initTable(): ResultSet = {
    session.execute(initTableStatement.bind())
  }

  def getKeyspaceName: String = {
    session.getLoggedKeyspace
  }
}
