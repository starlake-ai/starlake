package ai.starlake.schema.handlers

import org.apache.hadoop.fs.Path

import java.time.Instant

case class FileInfo(path: Path, fileSizeInBytes: Long, modificationInstant: Instant)
