package cassandra.changelog_manager.services

import java.io.{File, IOException}
import java.time.Instant
import java.util.Date

import cassandra.changelog_manager.cassandra.models.SchemaVersion
import cassandra.changelog_manager.testutil.MockGenerator._
import com.google.common.hash.HashCode
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

import scala.util.{Failure, Success, Try}

class ChangelogApplier_AlteredExecutedScripts_Test extends FlatSpec with MockitoSugar {

  class MockedChangelogApplier extends ChangelogApplier {
    override def getAlteredExecutedScripts(cqlFiles: List[File], appliedVersions: Set[SchemaVersion], alteredScripts: Set[String]): Set[(String, Try[Boolean])] = super.getAlteredExecutedScripts(cqlFiles, appliedVersions, alteredScripts)

    override protected def md5Hash(file: File): HashCode = HashCode.fromString(file.toString)
  }


  "ChangelogApplier.getAlteredExecutedScripts" should "return all altered executed scripts" in {
    //GIVEN
    val notExecutedScript = mockFileWithHashCode("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val notAlteredScript = mockFileWithHashCode("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")
    val alteredScript1 = mockFileWithHashCode("TestCQL3", "22c23862ee4f43c170fc41f7df8fce65")
    val alteredScript2 = mockFileWithHashCode("TestCQL4", "e2ee9c6add12d3c57b265c8f61a83255")

    val notInFilesSV = mockSchemaVersionNameAndHash("NotInFiles", "287690f6c81cbe07770cec41af87697c")
    val notAlteredSV = mockSchemaVersionNameAndHash("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")
    val alteredSV1 = mockSchemaVersionNameAndHash("TestCQL3", "9119eb5c43c1b6d9a56dc43acf07dce1")
    val alteredSV2 = mockSchemaVersionNameAndHash("TestCQL4", "0f0f88f95c0aa15aa37c19a67254deb9")

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(notExecutedScript, notAlteredScript, alteredScript1, alteredScript2),
      Set(notInFilesSV, notAlteredSV, alteredSV1, alteredSV2), Set())

    //THEN
    assert(result.equals(Set(("TestCQL3", Success(false)), ("TestCQL4", Success(false)))))
  }

  "ChangelogApplier.getAlteredExecutedScripts" should "return all altered executed scripts excepts thoses on alteredScriptList" in {
    //GIVEN
    val notExecutedScript = mockFileWithHashCode("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val notAlteredScript = mockFileWithHashCode("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")
    val alteredScript1 = mockFileWithHashCode("TestCQL3", "22c23862ee4f43c170fc41f7df8fce65")
    val alteredScript2 = mockFileWithHashCode("TestCQL4", "e2ee9c6add12d3c57b265c8f61a83255")

    val notInFilesSV = mockSchemaVersionNameAndHash("NotInFiles", "287690f6c81cbe07770cec41af87697c")
    val notAlteredSV = mockSchemaVersionNameAndHash("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")
    val alteredSV1 = mockSchemaVersionNameAndHash("TestCQL3", "9119eb5c43c1b6d9a56dc43acf07dce1")
    val alteredSV2 = mockSchemaVersionNameAndHash("TestCQL4", "0f0f88f95c0aa15aa37c19a67254deb9")

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(notExecutedScript, notAlteredScript, alteredScript1, alteredScript2),
      Set(notInFilesSV, notAlteredSV, alteredSV1, alteredSV2), Set("TestCQL3"))

    //THEN
    assert(result.equals(Set(("TestCQL4", Success(false)))))
  }

  "ChangelogApplier.getAlteredExecutedScripts" should "return nothing when no cql scripts" in {
    //GIVEN
    val sv1 = mockSchemaVersionNameAndHash("TestCQL3", "9119eb5c43c1b6d9a56dc43acf07dce1")
    val sv2 = mockSchemaVersionNameAndHash("TestCQL4", "0f0f88f95c0aa15aa37c19a67254deb9")

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(),
      Set(sv1, sv2), Set())

    //THEN
    assert(result.equals(Set()))
  }

  "ChangelogApplier.getAlteredExecutedScripts" should "return nothing when no already executed scripts" in {
    //GIVEN
    val script1 = mockFileWithHashCode("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val script2 = mockFileWithHashCode("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(script1, script2),
      Set(), Set())

    //THEN
    assert(result.equals(Set()))
  }

  "ChangelogApplier.getAlteredExecutedScripts" should "return nothing when all already executed scripts are not altered" in {
    //GIVEN
    val script1 = mockFileWithHashCode("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val script2 = mockFileWithHashCode("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")

    val sv1 = mockSchemaVersionNameAndHash("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val sv2 = mockSchemaVersionNameAndHash("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")

    val mockedChangelogApplier = spy(new MockedChangelogApplier)

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(script1, script2),
      Set(sv1, sv2), Set())

    //THEN
    assert(result.equals(Set()))
  }

  "ChangelogApplier.getAlteredExecutedScripts" should "make a faillure and continue executing when IO error" in {
    val exception = new IOException("Normal error")
    class MockedChangelogApplier(ioErrorFile: File) extends ChangelogApplier {

      override def getAlteredExecutedScripts(cqlFiles: List[File], appliedVersions: Set[SchemaVersion], alteredScripts: Set[String]): Set[(String, Try[Boolean])] = super.getAlteredExecutedScripts(cqlFiles, appliedVersions, alteredScripts)

      override protected def md5Hash(file: File): HashCode =
        if (file.getName.equals(ioErrorFile.getName)) {
          throw exception
        }
        else
          HashCode.fromString(file.toString)
    }
    //GIVEN
    val ioErrorFile = mockFileWithHashCode("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val someChecksumDifferFile = mockFileWithHashCode("TestCQL2", "7f5a87e1c715f672e43ee51a1de87d46")
    val someSuccessFile = mockFileWithHashCode("TestCQL3", "7f5a87e1c715f672e43ee51a1de87d46")

    val sv1 = mockSchemaVersionNameAndHash("TestCQL1", "287690f6c81cbe07770cec41af87697c")
    val sv2 = mockSchemaVersionNameAndHash("TestCQL2", "7e5a87e1c715f672e43ee51a1de87d46")

    val mockedChangelogApplier = spy(new MockedChangelogApplier(ioErrorFile))

    //WHEN
    val result = mockedChangelogApplier.getAlteredExecutedScripts(
      List(ioErrorFile, someChecksumDifferFile, someSuccessFile),
      Set(sv1, sv2), Set())

    //THEN
    assert(result.equals(Set((ioErrorFile.getName, Failure(exception)), (someChecksumDifferFile.getName, Success(false)))))
  }

  private def mockFileWithHashCode(fileName: String, hash: String): File = {
    val script = mockFile(fileName)
    when(script.toString).thenReturn(hash)
    script
  }

  private def mockSchemaVersionNameAndHash(fileName: String, hash: String): SchemaVersion = {
    SchemaVersion("", fileName, hash, "", Date.from(Instant.now()), 0, "")
  }
}
