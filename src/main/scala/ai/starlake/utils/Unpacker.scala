package ai.starlake.utils

import better.files.File
import com.github.ghik.silencer.silent
import org.apache.commons.compress.archivers.{
  ArchiveEntry,
  ArchiveInputStream,
  ArchiveStreamFactory
}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.io.input.CloseShieldInputStream

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.{Files, Paths}
import scala.util.Try

object Unpacker {

  def unpack(archiveFile: File, directory: File): Try[Unit] = {
    for {
      inputStream <- Try(Files.newInputStream(Paths.get(archiveFile.pathAsString)))
      it          <- open(inputStream)
    } yield {
      while (it.hasNext) {
        val (entry, is) = it.next()
        if (entry.isDirectory) {
          throw new Exception("Compressed archive cannot directories")
        }
        val targetFile = File(directory, entry.getName)
        val o = Files.newOutputStream(targetFile.path)
        try {
          IOUtils.copy(is, o)
        } finally {
          if (o != null) o.close()
        }
      }
    }
  }

  // https://alexwlchan.net/2019/09/unpacking-compressed-archives-in-scala/
  /** Unpack a compressed archive from an input stream; for example, a stream of bytes from a tar.gz
    * or tar.xz archive.
    *
    * The result is an iterator of 2-tuples, one for each entry in the archive:
    *
    *   - An ArchiveEntry instance, with information like name, size and whether the entry is a file
    *     or directory
    *   - An InputStream of all the bytes in this particular entry
    */
  def open(inputStream: InputStream): Try[Iterator[(ArchiveEntry, InputStream)]] =
    for {
      uncompressedInputStream <- createUncompressedStream(inputStream)
      archiveInputStream      <- createArchiveStream(uncompressedInputStream)
      iterator = createIterator(archiveInputStream)
    } yield iterator

  private def createUncompressedStream(inputStream: InputStream): Try[CompressorInputStream] =
    Try {
      new CompressorStreamFactory().createCompressorInputStream(
        getMarkableStream(inputStream)
      )
    }

  private def createArchiveStream(
    uncompressedInputStream: CompressorInputStream
  ): Try[ArchiveInputStream] =
    Try {
      new ArchiveStreamFactory()
        .createArchiveInputStream(
          getMarkableStream(uncompressedInputStream)
        )
    }

  private def createIterator(
    archiveInputStream: ArchiveInputStream
  ): Iterator[(ArchiveEntry, InputStream)] =
    new Iterator[(ArchiveEntry, InputStream)] {
      var latestEntry: ArchiveEntry = _

      override def hasNext: Boolean = {
        latestEntry = archiveInputStream.getNextEntry
        latestEntry != null
      }

      @silent
      override def next(): (ArchiveEntry, InputStream) =
        (latestEntry, new CloseShieldInputStream(archiveInputStream))
    }

  private def getMarkableStream(inputStream: InputStream): InputStream =
    if (inputStream.markSupported())
      inputStream
    else
      new BufferedInputStream(inputStream)

}
