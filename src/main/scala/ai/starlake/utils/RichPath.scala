package ai.starlake.utils

import org.apache.hadoop.fs.Path

object ImplicitRichPath {
  implicit class RichPath(path: Path) {
    def fileNameWithoutSlExt = {
      path.getName.replace(".sl.yml", "")
    }
  }

}
