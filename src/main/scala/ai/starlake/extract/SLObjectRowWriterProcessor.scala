package ai.starlake.extract

import com.typesafe.scalalogging.StrictLogging
import com.univocity.parsers.common.NormalizedString
import com.univocity.parsers.common.processor.ObjectRowWriterProcessor

import java.time.{Duration, Instant}

class SLObjectRowWriterProcessor(context: String)
    extends ObjectRowWriterProcessor
    with StrictLogging {

  private var recordsCount: Long = 0
  private var lastNotifiedTime: Instant = Instant.now()

  override def write(
    input: Array[AnyRef],
    headers: Array[NormalizedString],
    indexesToWrite: Array[Int]
  ): Array[AnyRef] = {
    recordsCount += 1
    if (Duration.between(lastNotifiedTime, Instant.now()).toSeconds > 30) {
      lastNotifiedTime = Instant.now()
      logger.info(
        s"$context Already extracted ${this.getRecordsCount()} rows"
      )
    }
    super.write(input, headers, indexesToWrite)
  }

  def getRecordsCount() = recordsCount
}
