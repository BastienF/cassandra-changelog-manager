package cassandra.changelog_manager

import java.io.File
import java.security.Permission

import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.mockito.MockitoSugar
import cassandra.database.CassandraDatabase
import org.mockito.Mockito.verify
import services.ChangelogApplier
import org.mockito.Mockito._

class MainMock(applier: ChangelogApplier, cassandraDatabase: CassandraDatabase, cqlDir: File, configFile: File) extends Main with MockitoSugar {
  override protected def initCassandraCluster(config: Config): CassandraDatabase = cassandraDatabase

  override protected def getApplier: ChangelogApplier = applier

  override protected def parseParameters: (File, File) = (cqlDir, configFile)
}

class Main_Test extends FlatSpec with MockitoSugar with BeforeAndAfterAll {

  sealed case class ExitException(status: Int) extends SecurityException(s"System.exit($status) called") {
  }

  sealed class NoExitSecurityManager extends SecurityManager {
    override def checkPermission(perm: Permission): Unit = {}

    override def checkPermission(perm: Permission, context: Object): Unit = {}

    override def checkExit(status: Int): Unit = {
      super.checkExit(status)
      throw ExitException(status)
    }
  }

  override def beforeAll(): Unit = {
    System.setSecurityManager(new NoExitSecurityManager())
  }

  override def afterAll(): Unit = {
    System.setSecurityManager(null)
  }

  "Main.run" should "load alteredFiles from conf" in {
    //GIVEN

    val givenConfig = new File("src/test/resources/conf/test_full.conf")
    println(givenConfig.getAbsolutePath)
    val mockedChangelogApplier = mock[ChangelogApplier]
    val mockedCassandraDatabase = mock[CassandraDatabase]
    val mockedChangelogDir = mock[File]
    val testedMain = new MainMock(mockedChangelogApplier, mockedCassandraDatabase, mockedChangelogDir, givenConfig)
    val alteredFilesInConf = Set("alteredScript.cql")
    when(mockedChangelogApplier.applyChangelogs(mockedCassandraDatabase, mockedChangelogDir, alteredFilesInConf)).thenReturn(true)

    //WHEN
    testedMain.run()

    //THEN
    verify(mockedChangelogApplier).applyChangelogs(mockedCassandraDatabase, mockedChangelogDir, alteredFilesInConf)
  }

  "Main.run" should "allow optional alteredFiles in conf" in {
    //GIVEN

    val givenConfig = new File("src/test/resources/conf/test_no_optional.conf")
    println(givenConfig.getAbsolutePath)
    val mockedChangelogApplier = mock[ChangelogApplier]
    val mockedCassandraDatabase = mock[CassandraDatabase]
    val mockedChangelogDir = mock[File]
    val testedMain = new MainMock(mockedChangelogApplier, mockedCassandraDatabase, mockedChangelogDir, givenConfig)
    val alteredFilesInConf: Set[String] = Set()
    when(mockedChangelogApplier.applyChangelogs(mockedCassandraDatabase, mockedChangelogDir, alteredFilesInConf)).thenReturn(true)

    //WHEN
    testedMain.run()

    //THEN
    verify(mockedChangelogApplier).applyChangelogs(mockedCassandraDatabase, mockedChangelogDir, alteredFilesInConf)
  }

}
