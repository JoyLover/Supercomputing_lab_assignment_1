package rdd

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp

import org.apache.spark.sql.functions.{avg, split, stddev_pop}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._
import java.text.SimpleDateFormat


object rdd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("lab_1_assignment")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val filePath = "src/main/resources/*500.gkg.csv"

    val raw_data = sc.textFile(filePath)

    val filterRDD = raw_data.map(_.split("\t", -1))
        .filter(_.length > 23)
        .map(x => (x(1), x(23)))
        .map {
          case (x, y) => (
            new Timestamp(new SimpleDateFormat("yyyyMMddHHmmss").parse(x).getTime).toString.split(" ")(0)
            , removeDuplicates(y))
        }
        .flatMap {case (x, y) => flatMatch(x, y)}
        .map {case (x, y, z) => ((x, y), z)}
        .filter {case ((x, y), z) => y != ""}
        .reduceByKey((x, y) => x + y)
        .map {case ((x, y), z) => (x, (y, z))}
        .sortBy(_._2._2, ascending = false)
        .groupByKey()


    case class nameCount(topic: String, count: Int)
    case class Results(date: String, result: List[nameCount])

    val json =
      (
        (filterRDD.collect().toList.map{
          case (x, y) =>
            ("date" -> x) ~
            ("result" -> y.take(10).map{
              case (a, b) =>
                ("topic" -> a) ~
                ("count" -> b)
            })
        })
      )

//    filterRDD.foreach(println)

    println()

    println(compact(render(json)))

    val fileOutput = new File("src/main/output.json")
    val bw = new BufferedWriter(new FileWriter(fileOutput))
    bw.write(compact(render(json)))
    bw.close()

    spark.stop()
  }

  def removeDuplicates (names: String): Set[String] = {

    val nameArray = names.split(";")
    val nameSet = collection.mutable.Set[String]()

    for (name <- nameArray) {
      nameSet.add(name.split(",")(0))
    }
    nameSet.toSet
  }

  def flatMatch (day: String, nameSet: Set[String]): Set[Tuple3[String, String, Int]] = {

    val nameTuple = collection.mutable.Set[Tuple3[String, String, Int]]()

    for (name <- nameSet) {
      nameTuple.add((day, name, 1))
    }
    nameTuple.toSet
  }
}
