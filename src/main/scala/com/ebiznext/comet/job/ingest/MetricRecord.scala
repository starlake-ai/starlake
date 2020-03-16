package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.utils.CometObjectMapper
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper

import scala.reflect.ClassTag

final case class CatCountFreq(category: String, count: Long, frequency: Double)

case class MetricRecord(
  domain: String,
  schema: String,
  attribute: String,
  min: Option[Long],
  max: Option[Long],
  mean: Option[Double],
  missingValues: Option[Long],
  standardDev: Option[Double],
  variance: Option[Double],
  sum: Option[Long],
  skewness: Option[Double],
  kurtosis: Option[Long],
  percentile25: Option[Long],
  median: Option[Long],
  percentile75: Option[Long],
  countDistinct: Option[Long],
  catCountFreq: Option[Seq[CatCountFreq]],
  missingValuesDiscrete: Option[Long],
  count: Long,
  cometTime: Long,
  cometStage: String
) {

  override def toString: String = {
    def seqOfMapStringOptToString[T](l: Seq[Map[String, Option[T]]]): String = {
      "[" + l
        .map(
          li =>
            "{" + li
              .map { case (k, v) => s"$k: ${v.map(_.toString).getOrElse("?")}" }
              .mkString(",") + "}"
        )
        .mkString(",") + "]"
    }

    Array(
      s"domain=$domain",
      s"schema=$schema",
      s"attribute=$attribute",
      s"min=${min.map(_.toString).getOrElse("?")}",
      s"max=${max.map(_.toString).getOrElse("?")}",
      s"mean=${mean.map(_.toString).getOrElse("?")}",
      s"missingValues=${missingValues.map(_.toString).getOrElse("?")}",
      s"standardDev=${standardDev.map(_.toString).getOrElse("?")}",
      s"variance=${variance.map(_.toString).getOrElse("?")}",
      s"sum=${sum.map(_.toString).getOrElse("?")}",
      s"skewness=${skewness.map(_.toString).getOrElse("?")}",
      s"kurtosis=${kurtosis.map(_.toString).getOrElse("?")}",
      s"percentile25=${skewness.map(_.toString).getOrElse("?")}",
      s"median=${median.map(_.toString).getOrElse("?")}",
      s"percentile75=${percentile75.map(_.toString).getOrElse("?")}",
      s"countDistinct=${countDistinct.map(_.toString).getOrElse("?")}",
      s"catCountFreq=${catCountFreq.map(_.toString).getOrElse("?")}",
      s"missingValuesDiscrete=${missingValuesDiscrete.map(_.toString).getOrElse("?")}",
      s"skewness=${skewness.map(_.toString).getOrElse("?")}",
      s"count=$count",
      s"cometTime=$cometTime",
      s"cometStage=$cometStage"
    ).mkString(",")
  }
}

object MetricRecord {

  import shapeless._

  /**
    * An SQL-side "avatar" for [[MetricRecord]]
    *
    * [[MetricRecord]] uses field types that are not directly representable in typical SQL files from Spark
    * (no Spark-compatible SQL dialect has maps, and only Postgres has at least some arrays but not quite enough
    * for what we need. This isn't quite enough)
    *
    * This record maps these to the corresponding SQL-compatible representation
    *
    * Note on Postgres: Postgres has a JSON data type. We suggest sticking to String/Varchar for the SQL interface,
    * and having the String-to-JSON conversions happen on the database side IF we have a use case for SQL queries
    * by a specific attribute of these JSON trees (as of writing this, I have a hunch there could be, but WAGNI until
    * proven otherwise -- cchepelov)
    */
  case class AsSql(
    domain: String,
    schema: String,
    attribute: String,
    min: Option[Long],
    max: Option[Long],
    mean: Option[Double],
    missingValues: Option[Long],
    standardDev: Option[Double],
    variance: Option[Double],
    sum: Option[Long],
    skewness: Option[Double],
    kurtosis: Option[Long],
    percentile25: Option[Long],
    median: Option[Long],
    percentile75: Option[Long],
    countDistinct: Option[Long],
    catCountFreq: Option[String],
    missingValuesDiscrete: Option[Long],
    count: Long,
    cometTime: Long,
    cometStage: String
  )

  /* The following are rather generic infrastructure stuff to map between MetricRecord and MetricRecord.AsSql.

  Without reuse, it's a little bit on the clunky side, but if there is reuse then we should find a correct place
  for that.


   */

  trait SqlCompatibleEncoder[T, U] {
    def toSqlCompatible(field: T): U

    def fromSqlCompatible(sqlSide: U): T
  }

  object SqlCompatibleEncoder {

    abstract class Identity[T] extends SqlCompatibleEncoder[T, T] {
      def toSqlCompatible(field: T): T = field

      def fromSqlCompatible(sqlSide: T): T = sqlSide
    }

    class UsingObjectMapper[T: ClassTag](mapper: ObjectMapper)
        extends SqlCompatibleEncoder[T, String] {
      override def toSqlCompatible(field: T): String =
        mapper.writeValueAsString(field)

      override def fromSqlCompatible(sqlSide: String): T =
        mapper.readValue(sqlSide, implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
    }

  }

  trait SqlCompatibleEncoders {

    import SqlCompatibleEncoder.Identity

    implicit object ForString extends Identity[String]

    implicit object ForBoolean extends Identity[Boolean]

    implicit object ForLong extends Identity[Long]

    implicit object ForDouble extends Identity[Double]

    case class ForOption[T, U]()(implicit inner: SqlCompatibleEncoder[T, U])
        extends SqlCompatibleEncoder[Option[T], Option[U]] {
      override def toSqlCompatible(field: Option[T]): Option[U] =
        field.map(inner.toSqlCompatible)

      override def fromSqlCompatible(sqlSide: Option[U]): Option[T] =
        sqlSide.map(inner.fromSqlCompatible)
    }

    implicit def forOption[T, U](
      implicit inner: SqlCompatibleEncoder[T, U]
    ): SqlCompatibleEncoder[Option[T], Option[U]] =
      ForOption[T, U]()
  }

  case class MetricFieldEncoders(mapper: ObjectMapper = new CometObjectMapper(new JsonFactory()))
      extends SqlCompatibleEncoders {

    import SqlCompatibleEncoder.UsingObjectMapper

    implicit object ForSeqString extends UsingObjectMapper[Seq[String]](mapper)

    implicit object ForSeqMapStringOptionLong
        extends UsingObjectMapper[Seq[Map[String, Option[Long]]]](mapper)

    implicit object ForSeqMapStringOptionDouble
        extends UsingObjectMapper[Seq[Map[String, Option[Double]]]](mapper)

    implicit object ForSeqCatCountFreq extends UsingObjectMapper[Seq[CatCountFreq]](mapper)

  }

  trait HListEncoder[TL <: HList, UL <: HList] {
    def toSqlCompatible(memSide: TL): UL

    def fromSqlCompatible(sqlSide: UL): TL
  }

  object HListEncoder {

    implicit object ForNone extends HListEncoder[HNil, HNil] {
      override def toSqlCompatible(memSide: HNil): HNil = HNil

      override def fromSqlCompatible(sqlSide: HNil): HNil = HNil
    }

    case class ForHCons[TH, TT <: HList, UH, UT <: HList]()(
      implicit hconv: SqlCompatibleEncoder[TH, UH],
      tconv: HListEncoder[TT, UT]
    ) extends HListEncoder[TH :: TT, UH :: UT] {
      override def toSqlCompatible(memSide: TH :: TT): UH :: UT = {
        val (mh :: mt) = memSide
        val sh = hconv.toSqlCompatible(mh)
        val sl = tconv.toSqlCompatible(mt)
        sh :: sl
      }

      override def fromSqlCompatible(sqlSide: UH :: UT): TH :: TT = {
        val (sh :: st) = sqlSide
        val mh = hconv.fromSqlCompatible(sh)
        val ml = tconv.fromSqlCompatible(st)
        mh :: ml
      }
    }

    implicit def forHCons[TH, TT <: HList, UH, UT <: HList](
      implicit hconv: SqlCompatibleEncoder[TH, UH],
      tconv: HListEncoder[TT, UT]
    ): HListEncoder[TH :: TT, UH :: UT] =
      ForHCons()
  }

  private case class GenericRecordConverter[T, U, TL <: HList, UL <: HList](
    tgen: Generic.Aux[T, TL],
    ugen: Generic.Aux[U, UL]
  )(implicit hle: HListEncoder[TL, UL]) {

    def toSqlCompatible(memValue: T): U = {
      val tl = tgen.to(memValue)
      val ul = hle.toSqlCompatible(tl)
      val u = ugen.from(ul)
      u
    }

    def fromSqlCompatible(sqlValue: U): T = {
      val ul = ugen.to(sqlValue)
      val tl = hle.fromSqlCompatible(ul)
      val t = tgen.from(tl)
      t
    }
  }

  case class MetricRecordConverter()
      extends SqlCompatibleEncoder[MetricRecord, MetricRecord.AsSql] {
    @transient private lazy val mapper: ObjectMapper = new CometObjectMapper()
    @transient private lazy val fieldEncoders = MetricFieldEncoders(mapper)
    import fieldEncoders._

    @transient private lazy val tgen = Generic[MetricRecord]
    @transient private lazy val ugen = Generic[MetricRecord.AsSql]
    @transient private lazy val internalConverter = GenericRecordConverter(tgen, ugen)

    override def toSqlCompatible(memSide: MetricRecord): MetricRecord.AsSql =
      internalConverter.toSqlCompatible(memSide)

    override def fromSqlCompatible(sqlSide: MetricRecord.AsSql): MetricRecord =
      internalConverter.fromSqlCompatible(sqlSide)
  }

}
