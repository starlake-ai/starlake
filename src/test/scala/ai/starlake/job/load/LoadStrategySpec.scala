package ai.starlake.job.load

import ai.starlake.TestHelper
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

class LoadStrategySpec extends TestHelper {
  new WithSettings {

    val myDataset1 = new Path(cometDatasetsPath + "/my_dataset_20210101120000.csv")
    val myDataset2 = new Path(cometDatasetsPath + "/my_dataset_20210102120000.csv")
    val myDataset3 = new Path(cometDatasetsPath + "/my_dataset_20210103120000.csv")

    "IngestionTimeStrategy" should "list files by modification_time and name" in {
      storageHandler.touchz(myDataset2)
      Thread.sleep(1000) // To have a different modification_time per file
      storageHandler.touchz(myDataset1)
      Thread.sleep(1000)
      storageHandler.touchz(myDataset3)

      val files: List[Path] = Utils
        .loadInstance[LoadStrategy]("ai.starlake.job.load.IngestionTimeStrategy")
        .list(storageHandler.fs, new Path(cometDatasetsPath), recursive = false)

      val expected: List[Path] = List(
        new Path(s"file:$cometDatasetsPath/my_dataset_20210102120000.csv"),
        new Path(s"file:$cometDatasetsPath/my_dataset_20210101120000.csv"),
        new Path(s"file:$cometDatasetsPath/my_dataset_20210103120000.csv")
      )
      files shouldEqual expected
    }

    "IngestionNameStrategy" should "list files by name" in {
      storageHandler.touchz(myDataset3)
      storageHandler.touchz(myDataset1)
      storageHandler.touchz(myDataset2)

      val files: List[Path] = Utils
        .loadInstance[LoadStrategy]("ai.starlake.job.load.IngestionNameStrategy")
        .list(storageHandler.fs, new Path(cometDatasetsPath), recursive = false)

      val expected: List[Path] = List(
        new Path(s"file:$cometDatasetsPath/my_dataset_20210101120000.csv"),
        new Path(s"file:$cometDatasetsPath/my_dataset_20210102120000.csv"),
        new Path(s"file:$cometDatasetsPath/my_dataset_20210103120000.csv")
      )
      files shouldEqual expected
    }
  }
}
