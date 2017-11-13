package cassandra.changelog_manager.cassandra.database

import cassandra.changelog_manager.cassandra.repository.SchemaVersionRepo
import com.datastax.driver.core.{Cluster, PlainTextAuthProvider, Session, SocketOptions}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters.{asJavaIterableConverter, collectionAsScalaIterableConverter}

class CassandraDatabase(config: Config) extends LazyLogging {
  private val hosts = config.getStringList("cassandra.hostnames").asScala.toSeq
  private val port = config.getInt("cassandra.port")
  private val username = config.getString("cassandra.username")
  private val password = config.getString("cassandra.password")
  private val adminKeyspace = config.getString("cassandra.admin_keyspace")
  private val keyspaces = config.getStringList("cassandra.keyspaces")

  private def clusterBuilder(builder: Cluster.Builder): (Cluster.Builder) = {
    def sslEnabler(): (Cluster.Builder) = {
      if (config.getBoolean("cassandra.ssl"))
        builder.withSSL()
      else
        builder
    }

    sslEnabler().withAuthProvider(new PlainTextAuthProvider(username, password))
  }

  logger.info(
    s"""
       |Building Cassandra connection with following configuration:
       |  hosts:          ${hosts.toList.asJava}
       |  port:           $port
       |  username:       $username
       |  password:       ${password.replaceAll(".", "*")}
       |  adminKeyspace:  $adminKeyspace
       |  keyspaces:      $keyspaces
     """.stripMargin)
  private val cluster: Cluster = clusterBuilder(Cluster.builder())
    .addContactPoints(hosts: _*)
    .withPort(port)
    .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(60000).setReadTimeoutMillis(60000))
    .build()
  private val adminSession: Session = cluster.connect(adminKeyspace)
  val keyspacesSessions: Seq[Session] = keyspaces.asScala.map(keyspace => cluster.connect(keyspace)).toSeq
  val schemaVersionRepo: SchemaVersionRepo = new SchemaVersionRepo(adminSession)

  def stop(): Unit = {
    adminSession.close()
    keyspacesSessions.foreach(_.close())
    cluster.close()
  }
}
