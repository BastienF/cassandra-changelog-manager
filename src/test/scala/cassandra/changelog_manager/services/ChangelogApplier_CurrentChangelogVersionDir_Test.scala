package cassandra.changelog_manager.services

import java.io.{File, IOException}
import java.time.Instant
import java.util.Date
import cassandra.changelog_manager.cassandra.models.SchemaVersion
import com.google.common.hash.HashCode
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import cassandra.changelog_manager.testutil.MockGenerator._
import scala.util.{Failure, Success, Try}

class ChangelogApplier_CurrentChangelogVersionDir_Test extends Specification with Mockito {

  class MockedChangelogApplier extends ChangelogApplier {
    override def currentChangelogVersionDir(changelogDir: File): File = super.currentChangelogVersionDir(changelogDir)
  }

  "ChangelogApplier.currentChangelogVersionDir" should {
    "return changelogDir when it doesn't contain any files" in {
      //GIVEN
      val changelogDir = mockContainingDir(List())

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual changelogDir
      success
    }

    "return changelogDir when it doesn't contain any version dir" in {
      //GIVEN
      val file1 = mockFile("moncql.cql")
      val dir1 = mockDir("v0a")
      val dir2 = mockDir("av02")
      val changelogDir = mockContainingDir(List(file1, dir1, dir2))

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual changelogDir
      success
    }

    "return version dir when it doesn't contain only one version dir" in {
      //GIVEN
      val file1 = mockFile("moncql.cql")
      val versionDir = mockDir("v10")
      val changelogDir = mockContainingDir(List(file1, versionDir))

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual versionDir
      success
    }

    "return latest version dir when it doesn't contain multiple version dir with simple numbers" in {
      //GIVEN
      val file1 = mockFile("moncql.cql")
      val oldVersionDir = mockDir("v1")
      val latestVersionDir = mockDir("v2")

      val changelogDir = mockContainingDir(List(file1, latestVersionDir, oldVersionDir))

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual latestVersionDir
      success
    }

    "return latest version dir when it doesn't contain multiple version dir with tricky numbers" in {
      //GIVEN
      val file1 = mockFile("moncql.cql")
      val latestVersionDir = mockDir("v100")
      val oldVersionDir = mockDir("v2")

      val changelogDir = mockContainingDir(List(file1, oldVersionDir, latestVersionDir))

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual latestVersionDir
      success
    }

    "return latest version dir when it doesn't contain multiple version dir with padded numbers" in {
      //GIVEN
      val file1 = mockFile("moncql.cql")
      val latestVersionDir = mockDir("v00100")
      val oldVersionDir = mockDir("v02")

      val changelogDir = mockContainingDir(List(file1, oldVersionDir, latestVersionDir))

      val mockedChangelogApplier = spy(new MockedChangelogApplier)

      //WHEN
      val result = mockedChangelogApplier.currentChangelogVersionDir(changelogDir)

      //THEN
      result shouldEqual latestVersionDir
      success
    }
  }
}
