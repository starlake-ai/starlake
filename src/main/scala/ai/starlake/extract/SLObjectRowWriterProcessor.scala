package ai.starlake.extract

import com.univocity.parsers.common.NormalizedString
import com.univocity.parsers.common.processor.ObjectRowWriterProcessor

class SLObjectRowWriterProcessor extends ObjectRowWriterProcessor {

  private var recordsCount: Long = 0

  override def write(
    input: Array[AnyRef],
    headers: Array[NormalizedString],
    indexesToWrite: Array[Int]
  ): Array[AnyRef] = {
    recordsCount += 1
    super.write(input, headers, indexesToWrite)
  }

  def getRecordsCount() = recordsCount
}
