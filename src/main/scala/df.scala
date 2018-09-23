package df

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.sql.{Date, Timestamp}


import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.functions.{collect_list,rank, explode,col,avg, split, stddev_pop,udf}
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable

case class Word(topic: String, count: Int) //We create a Word Object that will be used later to write the json file.

object df {
  //Creation of the object GdeltData that represent the Data we are going to exploit
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
  /* Main function : Compute the top 10 talked topics for each day
  *  from the records of the GdeltData
  */
  def main(args: Array[String]): Unit = {

    // The schema is the header of the DataFrame we are going to create.
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


    //Starting the Spark Session
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("lab_1_assignment")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val filePath = "src/main/resources/*500.gkg.csv"
    //Creating the Dataframe and importing the data from the resources files.
    val df = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("dateFormat", "yyyyMMddHHmmss")
      .csv(filePath)
      .as[GdeltData]

    //USer-Defined Function used to Split a String of the form : String,num in order to keep only the String
    val remove_udf = udf((p: String) => {
      p.split(",")(0)
    })
    //Computations on the DataFrame
    val dsPart = df.select("DATE", "AllNames")//Selecting the columns we need
      .withColumn("_tmp", split($"AllNames", ";"))//Splitting the String present in AllNames, so we have an array
      .drop("AllNames")//Deleting the old AllNames column
      .withColumn("NamesS", explode($"_tmp"))//We explode the array that was created in order to have one record for each topic
      .drop("_tmp")//Deleting intermediate column
      .withColumn("Names", remove_udf($"NamesS"))//Using the remove udf function on the topics in order to delete the internal count
      .drop("NamesS")
      .groupBy("DATE","Names").count()//Group the records by Date and Name and count the frequency of each topic per day
      .withColumn("rank", rank().over(Window.partitionBy("DATE").orderBy($"count".desc)))//Create a rank in order to sort the topic
      .filter($"rank" <= 10)//Keep only the Top 10 topics per day
      .drop("rank")//Deleting intermediate column

    //Function used to append the topic name and his count into a json object
    val zipper = udf((n: String, p: Int) => {

      implicit val formats = DefaultFormats
      write(Word(n,p))

    })

    //Creating the Json object for different days and writing it into a file
    dsPart.withColumn("result", zipper($"Names",$"count")).drop("Names").drop("count")
      .withColumnRenamed("DATE","date")
      .groupBy("date").agg(collect_list("result").alias("result")).write.json("src/main/output")



    //Closing the spark Session
    spark.stop()
  }
}
