package cassandra.changelog_manager.cassandra.models

import java.util.Date

case class SchemaVersion(app_version: String, script_name: String, checksum: String, executed_by: String, executed_on: Date, execution_time: Int, status: String)
