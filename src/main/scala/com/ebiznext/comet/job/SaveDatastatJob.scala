package com.ebiznext.comet.job




import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import better.files.{File, _}
import File._



/**  To record statistics with other information during ingestion.
  *
  */

object SaveDatastatJob {


    /** data Statistics information and status of the data
      *
      * @param domain                      : Domain name
      * @param schema                      : Schema
      * @param variableName                : Variable name from the attributes
      * @param minMetric                   : The minimum value associates with the variable
      * @param maxMetric                   : The maximum value associates with the variable
      * @param meanMetric                  : The mean value associates with the variable
      * @param countMetric                 : The count value associates with the variable
      * @param countMissValMetric          : The count value of missing values associates with the variable
      * @param varMetric                   : The variance value associates with the variable
      * @param stddevMetric                : The stddev value associates with the variable
      * @param sumMetric                   : The sum value associates with the variable
      * @param skewnessMetric              : The skewness value associates with the variable
      * @param kurtosisMetric              : The kurtosis value associates with the variable
      * @param percentile25Metric          : The percentile25 value associates with the variable
      * @param medianMetric                : The median value associates with the variable
      * @param percentile75Metric          : The percentile25 value associates with the variable
      * @param categoryMetric              : The category value associates with the variable (in the case of discrete variable)
      * @param countDiscreteMetric         : The count value associates with the variable (in the case of discrete variable)
      * @param frequenciesMetric           : The frequencies values associates with the variable (in the case of discrete variable)
      * @param CountMissValuesDiscrete     : The count value of missing values value associates with the variable (in the case of discrete variable)
      * @param timesIngestion              : The time at ingestion
      * @param stageState                  : The stage state  (consolidated or Ingested)
      */
    case class dataStatCaseClass( domain: String,
                                  schema: String,
                                  variableName: String,

                                  min: Option[Double],
                                  max: Option[Double],
                                  mean: Option[Double],
                                  count: Option[Long],
                                  missingValues: Option[Long],
                                  variance: Option[Double],
                                  standarddev: Option[Double],
                                  sum: Option[Double],
                                  skewness: Option[Double],
                                  kurtosis: Option[Double],
                                  percentile25: Option[Double],
                                  median: Option[Double],
                                  percentile75: Option[Double],

                                  category: Option[List[String]],
                                  countDisticnt: Option[Int],
                                  countBycategory: Option[Map[String, Long]],
                                  frequencies: Option[Map[String, Double]],

                                  missingValuesDiscrete: Option[Long],

                                  timesIngestion: String,
                                  stageState: String)


  /**
    * Spark configuration
    */

  val conf = new SparkConf()          // set the configuration
    .setAppName("Statistic Summary")
    .setMaster("local[*]")



  val spark = SparkSession           // init sparksession
    .builder
    .config(conf)
    .appName("readxlsx")
    .getOrCreate()

  import spark.implicits._

  /** Function to get values of option
    *
    * @param x
    * @return
    */

  def getShow(x: Option[Any]) = x match {
    case Some(s) => s
    case None => "?"
  }

  def myFunc(inVal: Option[Double]): Option[BigDecimal] = {
    Some(inVal.map(BigDecimal(_)).getOrElse(BigDecimal(0.0)))
  }




  /** Function that retrieves class names for each variable (case discrete variable)
    * @param dataInit     : Dataframe that contains all the computed metrics
    * @param nameVariable : name of the variable
    * @return             : list of all class associates to the variable
    */

  def getListCategory(dataInit: DataFrame , nameVariable : String) : (List[String] , Int) = {
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val listCategory: List[String]  = dataReduiceCategory.select("Category").collect.map(_.getString(0)).toList
    val lengthListCategory : Int = listCategory.length
    (listCategory, lengthListCategory)
  }

  /** Function that retrieves the values of the CountDiscrete metric (the distinct count) for one category  associate to one variable (case discrete variable)
    * @param dataInit      : Dataframe that contains all the computed metrics
    * @param nameVariable  : name of the variable
    * @param nameCategory  : name of the category
    * @return              : the value of the distinct count
    */

  def getMapCategoryCountDiscrete(dataInit: DataFrame , nameVariable : String, nameCategory : String): Long = {
    val metricName : String = "CountDiscrete"
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val dataCategory: DataFrame = dataReduiceCategory.filter(col("Category").isin(nameCategory))
    dataCategory.select(col(metricName)).first().getLong(0)
  }


  /** Function that retrieves the values of the Frequencies metric (frequency) for one category  associate to one variable (case discrete variable)
    * @param dataInit      : Dataframe that contains all the computed metrics
    * @param nameVariable  : name of the variable
    * @param nameCategory  : name of the category
    * @return              : the value of the frequency
    */

  def getMapCategoryFrequencies(dataInit: DataFrame , nameVariable : String, nameCategory : String): Double = {
    val metricName : String = "Frequencies"
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val dataCategory: DataFrame = dataReduiceCategory.filter(col("Category").isin(nameCategory))
    dataCategory.select(col(metricName)).first().getDouble(0)
  }




  /** Function to save the statDataFrame to parque the first time
    * @param dataToSave : statDataFrame
    * @param path       : the path directory
    * @return           : the resulting dataframe
    */

  def saveParquet(dataToSave: DataFrame, path : String) : DataFrame  = {
    dataToSave.coalesce(1).write.mode("append").parquet(path)
    dataToSave
  }

  /** Function to save the statDataFrame in the case it is not the first time
    * @param dataToSave : statDataFrame
    * @param path       : the path directory
    * @return           : the resulting dataframe
    */


  def saveDeleteParquet(dataToSave: DataFrame, path : String, saveDirectoryName: String) : DataFrame  = {
    val pathReel : String = path + "/" + saveDirectoryName + "/"
    val fileFinal: File = File(pathReel)

    val pathIntermediate : String = path + "/" + "intermediateDirectory" + "/"
    val fileIntermediate: File = File(pathIntermediate)

    val dataByVariableStored  : DataFrame = spark.read.parquet(pathReel).union(dataToSave)
    dataByVariableStored.coalesce(1).write.mode("append").parquet(pathIntermediate)
    fileFinal.delete()

    val dataStored: DataFrame = spark.read.parquet(pathIntermediate)
    dataStored.coalesce(1).write.mode("append").parquet(pathReel)
    fileIntermediate.delete()

    val dataFinal: DataFrame = spark.read.parquet(pathReel )
    dataFinal
  }


  /** Function to save the dfStatistics to parquet format.
    * @param dfStatistics      : Dataframe that contains all the computed metrics
    * @param domain            : name of the domain
    * @param schema            : schema of the initial data
    * @param timeIngestion     : time which correspond to the ingestion
    * @param stageState        : stage
    * @param path              : path where to save the file
    * @param saveDirectoryName : The name of the saving directory
    * @param threshold         : The limit value for the number of sub-class to consider
    * @return                  : the stored dataframe version of the parquet file
    */


  def saveDataStatToParquet(dfStatistics: DataFrame, domain: String, schema: String, timeIngestion : String, stageState : String , path : String, saveDirectoryName: String,  threshold : Int) : DataFrame = {


    val listVariable : List[String]= dfStatistics.select("Variables").select("Variables").collect.map(_.getString(0)).toList.distinct
    val listRowByVariable : List[SaveStatistics.dataStatCaseClass]= listVariable.map(c => dataStatCaseClass(
      domain,
      schema,
      c,

      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Min").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Max").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Mean").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Count").first().getLong(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("CountMissValues").first().getLong(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Var").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Stddev").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Sum").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Skewness").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Kurtosis").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Percentile25").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Median").first().getDouble(0))
      case _ => None},
      dfStatistics.filter(col("Variables").isin(c)).count()  match { case 1 => Some(dfStatistics.filter(col("Variables").isin(c)).select("Percentile75").first().getDouble(0))
      case _ => None},



      dfStatistics.filter(col("Variables").isin(c)).count()  match {
        case 1 => None
        case _ => Some(dfStatistics.filter(col("Variables").isin(c)).select("Category").collect.map(_.getString(0)).toList)},

      dfStatistics.filter(col("Variables").isin(c)).count()  match {
        case 1 => None
        case _ => Some(getListCategory(dfStatistics, c)_2)},

      dfStatistics.filter(col("Variables").isin(c)).count()  match {
        case 1 => None
        case _ => (getListCategory(dfStatistics, c)_2) - threshold < 0 match {
          case true => Some(Map((getListCategory(dfStatistics, c)._1).map(x => (x -> getMapCategoryCountDiscrete(dfStatistics, c, x))): _*))
          case false => None }
      },

      dfStatistics.filter(col("Variables").isin(c)).count()  match {
        case 1 => None
        case _ => (getListCategory(dfStatistics, c)_2) -threshold < 0 match {
          case true => Some(Map((getListCategory(dfStatistics, c)._1).map(x => (x -> getMapCategoryFrequencies(dfStatistics, c, x))): _*))
          case false => None
        }
      },


      dfStatistics.filter(col("Variables").isin(c)).count()  match {
        case 1 => None
        case _ => (getListCategory(dfStatistics, c)_2) - threshold < 0 match {
          case true => Some(dfStatistics.filter(col("Variables").isin(c)).select("CountMissValuesDiscrete").first().getLong(0))
          case false => None
        }
      },

      timeIngestion,
      stageState))

    val dataByVariable   = listRowByVariable.toDF
    val pathReel : String = path + "/" + saveDirectoryName + "/"
    val fileFinal: File = File(pathReel)

    fileFinal.isEmpty  match {
      case true  => saveParquet(dataByVariable, pathReel)
      case false => saveDeleteParquet(dataByVariable, path, saveDirectoryName)
    }


  }










}


