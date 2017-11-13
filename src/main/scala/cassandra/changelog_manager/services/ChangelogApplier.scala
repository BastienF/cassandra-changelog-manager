package cassandra.changelog_manager.services

import java.io.{File, IOException}
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.database.CassandraDatabase
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import com.google.common.hash.{HashCode, Hashing}
import com.google.common.io.{ByteSource, Files}
import com.typesafe.scalalogging.LazyLogging
import cassandra.changelog_manager.utils.CqlExecutionException
import scala.io.Source
import scala.util.{Success, Try}

object ChangelogApplier extends ChangelogApplier

protected abstract class ChangelogApplier extends LazyLogging {

  def applyChangelogs(cassandraDatabase: CassandraDatabase, changelogDir: File): Boolean = {
    cassandraDatabase.schemaVersionRepo.initTable()
    val appliedVersions = cassandraDatabase.schemaVersionRepo.getAll
    val cqlFiles = currentChangelogVersionDir(changelogDir).listFiles.filter(_.isFile).toList
    val alteredScripts = Try(getAlteredExecutedScripts(cqlFiles, appliedVersions))


    if (alteredScripts.get.nonEmpty) {
      alteredScripts.get.foreach(fileNameStatus =>
        if (fileNameStatus._2.isSuccess)
          logger.error(s"checksum differs for: ${fileNameStatus._1}")
        else
          logger.error(
            s"""IO error for ${fileNameStatus._1}:
               |${fileNameStatus._2.failed.get}""".stripMargin)
      )
      false
    }
    else {
      val executionResult: Try[Unit] = Try(notAlreadyApplied(cqlFiles, appliedVersions).sortBy(f => f.getName) foreach (cql => {
        val result = executeCQLStatements(cql, cassandraDatabase)
      }))
      executionResult.isSuccess
    }
  }

  protected def currentChangelogVersionDir(changelogDir: File): File = {
    val versionDirectories: Set[File] = changelogDir.listFiles().filter(_.isDirectory).filter(dir => dir.getName.matches("^v[0-9]+$")).toSet
    if (versionDirectories.isEmpty)
      changelogDir
    else
      versionDirectories.maxBy(dir => dir.getName.substring(1).toInt)
  }

  @throws[IOException]
  protected def getAlteredExecutedScripts(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): Set[(String, Try[Boolean])] = {
    appliedVersions
      .map(appliedVersion =>
        cqlFiles
          .find(f => f.getName.equals(appliedVersion.script_name))
          .map(f => (f.getName, Try(md5Hash(f).toString.equals(appliedVersion.checksum))))
          .getOrElse(appliedVersion.script_name, Success(true)))
      .filterNot(_._2.getOrElse(false))
  }

  @throws[CqlExecutionException]
  protected def executeCQLStatements(cqlFile: File, cassandraDatabase: CassandraDatabase): Boolean = {
    val queries = cqlScriptToQueries(readFile(cqlFile))
    cassandraDatabase.keyspacesSessions.foreach(session => {

      queries.foreach(query => {
        try {
          session.execute(session.prepare(query).bind())
        } catch {
          case ex: Exception =>
            logger.error(
              s"""Failure on ${cqlFile.getName}:
                 |  failed query: $query
                 |  exception: $ex""".stripMargin)
            val exception = CqlExecutionException(cqlFile.getName, session.getLoggedKeyspace, ex)
            throw exception
        }
      }
      )
    }
    )
    logger.info(s"${cqlFile.getName} executed with success")
    try {
      cassandraDatabase.schemaVersionRepo.insert(SchemaVersion("v0", cqlFile.getName, md5Hash(cqlFile).toString, "", getCurrentDate, 0, "success"))
    } catch {
      case ex: Exception =>
        logger.error(
          s"""Failure on admin keyspace update for ${cqlFile.getName} execution:
             |  $ex""".stripMargin)
        val exception = CqlExecutionException(cqlFile.getName, cassandraDatabase.schemaVersionRepo.getKeyspaceName, ex)
        throw exception
    }
    true
  }

  protected def cqlScriptToQueries(script: String): List[String] = {
    script.split("\\s*-- cassandra-changelog-manager statement\\s*\n").map(_.trim).filterNot(_.isEmpty).toList
  }

  protected def notAlreadyApplied(cqlFiles: List[File], appliedVersions: Set[SchemaVersion]): List[File] = {
    cqlFiles.filter(cql => appliedVersions.forall(appliedVersion => appliedVersion.script_name != cql.getName))
  }

  protected def readFile(file: File): String = {
    val source = Source.fromFile(file)
    try source.mkString finally source.close()
  }

  protected def getCurrentDate: Date = {
    Date.from(Instant.now())
  }

  @throws[IOException]
  protected def md5Hash(file: File): HashCode = getFileAsByteSource(file).hash(Hashing.md5())

  protected def getFileAsByteSource(file: File): ByteSource = {
    Files.asByteSource(file)
  }
}
