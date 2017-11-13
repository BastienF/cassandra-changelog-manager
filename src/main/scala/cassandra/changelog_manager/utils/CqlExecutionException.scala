package cassandra.changelog_manager.utils

case class CqlExecutionException(cqlFileName: String, keyspace: String, error: Exception) extends Exception
