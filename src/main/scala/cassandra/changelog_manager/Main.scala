package cassandra.changelog_manager

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import cassandra.database.CassandraDatabase
import services.ChangelogApplier
import net.ceedubs.ficus.Ficus._

import scala.util.Try

object Main extends Main {
  override protected def getApplier: ChangelogApplier = ChangelogApplier

  def main(args: Array[String]): Unit = run()
}

abstract class Main extends LazyLogging {

  protected def getApplier: ChangelogApplier

  def run(): Unit = {

    val (changelogDir: File, configFile: File) = parseParameters

    // Allowing user to override conf file with -Dproperty
    val config: Config = ConfigFactory.load().withFallback(ConfigFactory.parseFile(configFile).getConfig("conf"))

    val cluster: CassandraDatabase = initCassandraCluster(config)
    applyChangelogs(changelogDir, config, cluster)
    logger.info("Changelog successfully executed")
  }

  protected def parseParameters: (File, File) = {
    val configPath = System.getProperty("config")
    val changelogPath = System.getProperty("changelog")
    if (configPath == null || changelogPath == null) {
      logger.error(
        """Mandatory parameters:
          |-Dconfig=<path>    (path of the config file)
          |-Dchangelog=<path> (path of the directory containing the cql files)""".stripMargin)
      sys.exit(1)
    }

    val changelogDir = new File(changelogPath)
    val configFile = new File(configPath)

    if (!changelogDir.exists || !changelogDir.isDirectory) {
      logger.error("changelog parameter has to be a directory path")
      sys.exit(2)
    }

    if (!configFile.exists || !configFile.isFile) {
      logger.error("config parameter has to be a file path")
      sys.exit(2)
    }

    logger.info(
      s"""
         |Running Cassandra changelog manager with following parameters
         |  config path:              $configFile
         |  changelog directory path: ${changelogDir.getAbsolutePath}
       """.stripMargin)

    (changelogDir, configFile)
  }

  protected def applyChangelogs(changelogDir: File, config: Config, cluster: CassandraDatabase): Unit = {
    val alteredScripts = config.as[Option[Set[String]]]("changelog_applier.altered_scripts").getOrElse(Set())
    val result = getApplier.applyChangelogs(cluster, changelogDir, alteredScripts)
    cluster.stop()

    if (!result) {
      logger.error("Execution failed.")
      sys.exit(3)
    }
  }

  protected def initCassandraCluster(config: Config): CassandraDatabase = {
    val cluster = Try(new CassandraDatabase(config))
    if (cluster.isFailure) {
      logger.error(
        s"""Cassandra connection failure:
           |  ${cluster.failed.get}
         """.stripMargin)
      sys.exit(4)
    }
    cluster.get
  }
}
