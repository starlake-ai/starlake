package com.ebiznext.comet.schema.handlers

import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

trait StorageHandler {

  def move(path: Path, path1: Path): Boolean

  def delete(path: Path)

  def mkdirs(path: Path)

  def copyFromLocal(source: Path, dest: Path): Unit

  def moveFromLocal(source: Path, dest: Path): Unit

  def read(path: Path): String

  def write(data: String, path: Path): Unit

  def list(path: Path,
           extension: String = "",
           since: LocalDateTime = LocalDateTime.MIN): List[Path]
}

class HdfsStorageHandler extends StorageHandler {
  implicit def convertToScalaIterator[T](
                                          underlying: RemoteIterator[T]): Iterator[T] = {
    case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
      override def hasNext = underlying.hasNext

      override def next = underlying.next
    }
    wrapper(underlying)
  }

  def read(path: Path): String = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val stream = fs.open(path)
    val content = IOUtils.toString(stream, "UTF-8")
    content
  }

  def write(data: String, path: Path): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.delete(path, false)
    val os: FSDataOutputStream = fs.create(path)
    os.writeBytes(data)
    os.close()
  }

  def list(path: Path, extension: String, since: LocalDateTime): List[Path] = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val iterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, false)
    iterator
      .filter { status =>
        val time = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(status.getModificationTime),
          ZoneId.systemDefault)
        time.isAfter(since) && status.getPath().getName().endsWith(extension)
      }
      .map(status => status.getPath())
      .toList
  }


  override def move(path: Path, dest: Path): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    FileUtil.copy(fs, path, fs, dest, true, true, conf)
  }

  override def delete(path: Path): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.delete(path, true)
  }

  override def mkdirs(path: Path): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.mkdirs(path)
  }

  override def copyFromLocal(source: Path, dest: Path): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.copyFromLocalFile(source, dest)
  }

  override def moveFromLocal(source: Path, dest: Path): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.moveFromLocalFile(source, dest)
  }
}
