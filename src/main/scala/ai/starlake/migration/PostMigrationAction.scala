package ai.starlake.migration
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

sealed trait PostMigrationAction extends StrictLogging {
  def process(): Try[Unit]
}

class RenameFolder(folder: Path, newName: String)(implicit storageHandler: StorageHandler)
    extends PostMigrationAction {

  override def process(): Try[Unit] = {
    logger.info(s"Renaming $folder as $newName")
    val targetPath = new Path(folder.getParent, newName)
    if (storageHandler.exists(folder)) {
      if (storageHandler.move(folder, targetPath)) {
        Success(())
      } else {
        val errorMessage = s"Could not rename $folder"
        logger.error(errorMessage)
        Failure(new RuntimeException(errorMessage))
      }
    } else if (!storageHandler.exists(targetPath)) {
      val errorMessage =
        s"$folder and its target $targetPath don't exists. Problem may have occurs during migration process."
      logger.error(errorMessage)
      Failure(new RuntimeException(errorMessage))
    } else {
      Success(())
    }
  }
}
