//package main
//
//import org.apache.spark.sql.types._
//import org.apache.spark.sql._
//import org.apache.log4j.{Level, Logger}
//import java.sql.Timestamp
//import org.apache.spark.sql.functions.{split, avg, stddev_pop}
//
//object main {
//
//  case class GdeltData (
//    GKGRECORDID                 : String,
//    DATE                        : Timestamp,
//    SourceCollectionIdentifier  : Integer,
//    SourceCommonName            : String,
//    DocumentIdentifier          : String,
//    Counts                      : String,
//    V2Counts                    : String,
//    Themes                      : String,
//    V2Themes                    : String,
//    Locations                   : String,
//    V2Locations                 : String,
//    Persons                     : String,
//    V2Persons                   : String,
//    Organizations               : String,
//    V2Organizations             : String,
//    V2Tone                      : String,
//    Dates                       : String,
//    GCAM                        : String,
//    SharingImage                : String,
//    RelatedImages               : String,
//    SocialImageEmbeds           : String,
//    SocialVideoEmbeds           : String,
//    Quotations                  : String,
//    AllNames                    : String,
//    Amounts                     : String,
//    TranslationInfo             : String,
//    Extras                      : String
//  )
//
//  def main(args: Array[String]): Unit = {
//
//    val schema =
//      StructType(
//        Array(
//          StructField("GKGRECORDID"                 , StringType,     nullable = true),
//          StructField("DATE"                        , TimestampType,  nullable = true),
//          StructField("SourceCollectionIdentifier"  , IntegerType,    nullable = true),
//          StructField("SourceCommonName"            , StringType,     nullable = true),
//          StructField("DocumentIdentifier"          , StringType,     nullable = true),
//          StructField("Counts"                      , StringType,     nullable = true),
//          StructField("V2Counts"                    , StringType,     nullable = true),
//          StructField("Themes"                      , StringType,     nullable = true),
//          StructField("V2Themes"                    , StringType,     nullable = true),
//          StructField("Locations"                   , StringType,     nullable = true),
//          StructField("V2Locations"                 , StringType,     nullable = true),
//          StructField("Persons"                     , StringType,     nullable = true),
//          StructField("V2Persons"                   , StringType,     nullable = true),
//          StructField("Organizations"               , StringType,     nullable = true),
//          StructField("V2Organizations"             , StringType,     nullable = true),
//          StructField("V2Tone"                      , StringType,     nullable = true),
//          StructField("Dates"                       , StringType,     nullable = true),
//          StructField("GCAM"                        , StringType,     nullable = true),
//          StructField("SharingImage"                , StringType,     nullable = true),
//          StructField("RelatedImages"               , StringType,     nullable = true),
//          StructField("SocialImageEmbeds"           , StringType,     nullable = true),
//          StructField("SocialVideoEmbeds"           , StringType,     nullable = true),
//          StructField("Quotations"                  , StringType,     nullable = true),
//          StructField("AllNames"                    , StringType,     nullable = true),
//          StructField("Amounts"                     , StringType,     nullable = true),
//          StructField("TranslationInfo"             , StringType,     nullable = true),
//          StructField("Extras"                      , StringType,     nullable = true)
//        ))
//
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//
//    val spark = SparkSession
//      .builder
//      .appName("lab_1_assignment")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    import spark.implicits._
//    val sc = spark.sparkContext
//
//    val filePath = "src/main/resources/*.gkg.csv"
//
//    val ds = spark.read
//      .schema(schema)
//      .option("delimiter", "\t")
//      .option("timestampFormat", "yyyyMMddHHmmss")
//      .csv(filePath)
//      .as[GdeltData]
//
//    // Select the columns we need
//    val dsPart = ds.select("GKGRECORDID", "SourceCollectionIdentifier", "SourceCommonName",
//      "DocumentIdentifier", "DATE", "V2Tone", "GCAM")
//      .withColumn("_tmp", split($"V2Tone", ","))
//      .select($"GKGRECORDID", $"SourceCollectionIdentifier", $"SourceCommonName", $"DocumentIdentifier", $"DATE"
//        , $"_tmp".getItem(0).as("tone"), $"GCAM")
//
//    /**
//      * Filter the themes that we think are negative
//      */
//    val dsKill = dsPart.filter($"GCAM".contains("c18.1:"))        // c18.1	  KILL
//    val dsRape = dsPart.filter($"GCAM".contains("c18.115:"))      // c18.115  RAPE
//    val dsElection = dsPart.filter($"GCAM".contains("c18.140:"))  // c18.140  ELECTION
//    val dsGrugTrade = dsPart.filter($"GCAM".contains("c18.146:")) // c18.146  DRUG_TRADE
//    val dsLGBT = dsPart.filter($"GCAM".contains("c18.111:"))      // c18.111  LGBT
//
//    /**
//      * Filter the themes that we think are positive
//      */
//    val dsAgri = dsPart.filter($"GCAM".contains("c18.52:"))       // c18.52   AGRICULTURE
//    val dsSolarPwr = dsPart.filter($"GCAM".contains("c18.168"))   // c18.168  ENV_SOLAR
//    val dsPeaceBld = dsPart.filter($"GCAM".contains("c18.216:"))  // c18.216  SLFID_PEACE_BUILDING
//    val dsFreeTrade = dsPart.filter($"GCAM".contains("c18.246:")) // c18.246  ECON_FREETRADE
//    val dscEconcoop = dsPart.filter($"GCAM".contains("c18.272:")) // c18.272  SOC_ECONCOOP, economic cooperation
//
//    println("The total number of records from the dataset:\t" + dsPart.count())
//
//    println()
//
//    println("The number of Event KILL:\t" + dsKill.count())
//    println("The number of Event RAPE:\t" + dsRape.count())
//    println("The number of Event ELECTION:\t" + dsElection.count())
//    println("The number of Event DRUG_TRADE:\t" + dsGrugTrade.count())
//    println("The number of Event LGBT:\t" + dsLGBT.count())
//
//    println()
//
//    println("The number of Event AGRICULTURE:\t" + dsAgri.count())
//    println("The number of Event ENV_SOLAR:\t" + dsSolarPwr.count())
//    println("The number of Event SLFID_PEACE_BUILDING:\t" + dsPeaceBld.count())
//    println("The number of Event FREE_TRADE:\t" + dsFreeTrade.count())
//    println("The number of Event ECONOMIC_COOPERATION:\t" + dscEconcoop.count())
//
//    println()
//
//    /**
//      * Average and standard deviation of tone from themes we think are negative
//      */
//    dsKill.select(avg($"tone").as("kill_avg_tone"), stddev_pop($"tone").as("kill_std_tone")).show()
//    dsRape.select(avg($"tone").as("rape_avg_tone"), stddev_pop($"tone").as("rape_std_tone")).show()
//    dsElection.select(avg($"tone").as("election_avg_tone"), stddev_pop($"tone").as("election_std_tone")).show()
//    dsGrugTrade.select(avg($"tone").as("drugtrade_avg_tone"), stddev_pop($"tone").as("drugtrade_std_tone")).show()
//    dsLGBT.select(avg($"tone").as("lgbt_avg_tone"), stddev_pop($"tone").as("lgbt_std_tone")).show()
//
//    println()
//
//    /**
//      * Average and standard deviation of tone from themes we think are positive
//      */
//    dsAgri.select(avg($"tone").as("agriculture_avg_tone"), stddev_pop($"tone").as("agriculture_std_tone")).show()
//    dsSolarPwr.select(avg($"tone").as("solar_avg_tone"), stddev_pop($"tone").as("solar_std_tone")).show()
//    dsPeaceBld.select(avg($"tone").as("peacebld_avg_tone"), stddev_pop($"tone").as("peacebld_std_tone")).show()
//    dsFreeTrade.select(avg($"tone").as("freetrade_avg_tone"), stddev_pop($"tone").as("freetrade_std_tone")).show()
//    dscEconcoop.select(avg($"tone").as("econcoop_avg_tone"), stddev_pop($"tone").as("econcoop_std_tone")).show()
//
//    println()
//
//    // Records the event website of abnormal theme "AGRICULTURE"
//    println("'AGRICULTURE':")
//    dsAgri.filter($"SourceCollectionIdentifier" === 1 && $"tone" <= -4)
//      .select("SourceCommonName", "tone", "DocumentIdentifier")
//      .collect.take(5).foreach(println)
//
//    println()
//
//    println("'SLFID_PEACE_BUILDING'")
//    // Records the event website of abnormal theme "SLFID_PEACE_BUILDING"
//    dsPeaceBld.filter($"SourceCollectionIdentifier" === 1 && $"tone" <= -4)
//      .select("SourceCommonName", "tone", "DocumentIdentifier")
//      .collect.take(5).foreach(println)
//
//    println()
//
//    println("'LGBT':")
//    // Records the event website of abnormal theme "LGBT"
//    dsLGBT.filter($"SourceCollectionIdentifier" === 1 && $"tone" >= 2)
//      .select("SourceCommonName", "tone", "DocumentIdentifier")
//      .collect.take(5).foreach(println)
//
//    println()
//
//    /**
//      * See which GDELT GKG Themes is extremely positive and negative
//      */
//    val dsPos = dsPart.filter($"tone" >= 15).select("SourceCommonName", "tone", "GCAM", "DocumentIdentifier")
//    val dsNega = dsPart.filter($"tone" <= -15).select("SourceCommonName", "tone", "GCAM", "DocumentIdentifier")
//
////    dsPos.collect.foreach(println)
////    dsNega.collect.foreach(println)
//
//    println("The number extremely positive events:\t" + dsPos.count())
//    println("The number extremely negative events:\t" + dsNega.count())
//
//    dsPos.write.mode("overwrite").csv("src/main/outputs/positive_event.csv")
//    dsNega.write.mode("overwrite").csv("src/main/outputs/negative_event.csv")
//
//    spark.stop()
//  }
//}
