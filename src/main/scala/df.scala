package df

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions.{avg, split, stddev_pop}

import scala.collection.mutable

object df {

  case class GdeltData (
    GKGRECORDID                 : String,
    DATE                        : Date,
    SourceCollectionIdentifier  : Integer,
    SourceCommonName            : String,
    DocumentIdentifier          : String,
    Counts                      : String,
    V2Counts                    : String,
    Themes                      : String,
    V2Themes                    : String,
    Locations                   : String,
    V2Locations                 : String,
    Persons                     : String,
    V2Persons                   : String,
    Organizations               : String,
    V2Organizations             : String,
    V2Tone                      : String,
    Dates                       : String,
    GCAM                        : String,
    SharingImage                : String,
    RelatedImages               : String,
    SocialImageEmbeds           : String,
    SocialVideoEmbeds           : String,
    Quotations                  : String,
    AllNames                    : String,
    Amounts                     : String,
    TranslationInfo             : String,
    Extras                      : String
  )

  def main(args: Array[String]): Unit = {

    val schema =
      StructType(
        Array(
          StructField("GKGRECORDID"                 , StringType,     nullable = true),
          StructField("DATE"                        , DateType,       nullable = true),
          StructField("SourceCollectionIdentifier"  , IntegerType,    nullable = true),
          StructField("SourceCommonName"            , StringType,     nullable = true),
          StructField("DocumentIdentifier"          , StringType,     nullable = true),
          StructField("Counts"                      , StringType,     nullable = true),
          StructField("V2Counts"                    , StringType,     nullable = true),
          StructField("Themes"                      , StringType,     nullable = true),
          StructField("V2Themes"                    , StringType,     nullable = true),
          StructField("Locations"                   , StringType,     nullable = true),
          StructField("V2Locations"                 , StringType,     nullable = true),
          StructField("Persons"                     , StringType,     nullable = true),
          StructField("V2Persons"                   , StringType,     nullable = true),
          StructField("Organizations"               , StringType,     nullable = true),
          StructField("V2Organizations"             , StringType,     nullable = true),
          StructField("V2Tone"                      , StringType,     nullable = true),
          StructField("Dates"                       , StringType,     nullable = true),
          StructField("GCAM"                        , StringType,     nullable = true),
          StructField("SharingImage"                , StringType,     nullable = true),
          StructField("RelatedImages"               , StringType,     nullable = true),
          StructField("SocialImageEmbeds"           , StringType,     nullable = true),
          StructField("SocialVideoEmbeds"           , StringType,     nullable = true),
          StructField("Quotations"                  , StringType,     nullable = true),
          StructField("AllNames"                    , StringType,     nullable = true),
          StructField("Amounts"                     , StringType,     nullable = true),
          StructField("TranslationInfo"             , StringType,     nullable = true),
          StructField("Extras"                      , StringType,     nullable = true)
        ))

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("lab_1_assignment")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val filePath = "src/main/resources/20150218230000.gkg.csv"

    val ds = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("dateFormat", "yyyyMMddHHmmss")
      .csv(filePath)
      .as[GdeltData]

    case class DuplicateRemoved(DATE: String,
                                products: mutable.WrappedArray[String],
                                rm_duplicates: mutable.WrappedArray[String])

    // Select the columns we need
    val dsPart = ds.select("DATE", "AllNames")
      .withColumn("_tmp", split($"AllNames", ";"))
      .drop("AllNames")
//      .map{case x =>
//        (x, removeDuplicates(x(1).asInstanceOf(String)))}
//        .toDF()
//      .groupBy("DATE")

    val namesDic = collection.mutable.Map[Date, collection.mutable.Map[String, Int]]()

//    dsPart.printSchema()
    println()



//    dsPart.collect.foreach(println)
//    dsPart.show()


    spark.stop()
  }

  def removeDuplicates (names: Array[String]): Set[String] = {

    val nameSet = collection.mutable.Set[String]()

    for (name <- names) {
      nameSet.add(name.split(",")(0))
    }
    nameSet.toSet
  }
}
