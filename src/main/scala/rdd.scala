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

    val filePath = "src/main/resources/*.gkg.csv"

    val raw_data = sc.textFile(filePath)

    // Main steps.
    val filterRDD = raw_data.map(_.split("\t", -1))   // Split the row to list of strings.
        .filter(_.length > 23)  // Discard the row of which the length is less than 23 to avoid unexpected errors.
        .map(x => (x(1), x(23)))  // Select the "DATE" and "AllNames" columns.
        .map {    // Remove the duplicates in "AllNames" value within each record and change the Timestamp type to Date type.
          case (x, y) => (
            new Timestamp(new SimpleDateFormat("yyyyMMddHHmmss").parse(x).getTime).toString.split(" ")(0)
            , removeDuplicates(y))
        }
        // Flat the key-value pair to match key to every element in the value set. 
        // The 1st entry is "DATE", the 2nd is "AllNames" and the 3rd is the number of occurances of each topic.
        .flatMap {case (x, y) => flatMatch(x, y)} 
        .map {case (x, y, z) => ((x, y), z)}    // Reconstruct the structure.
        .filter {case ((x, y), z) => y != ""}   // Filter record with empty topic.
        .reduceByKey((x, y) => x + y)           // Computer the number of occurances of each topic every event.
        .map {case ((x, y), z) => (x, (y, z))}  // Reconstruct the structure.
        .sortBy(_._2._2, ascending = false)     // Descend by number of occurances of each topic.
        .groupByKey()                           // Group by "DATE".

    // Used for json format.
    case class nameCount(topic: String, count: Int)
    case class Results(date: String, result: List[nameCount])

    // Map every field to its name.
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

    println()

    println(compact(render(json)))

    val fileOutput = new File("src/main/output.json")
    val bw = new BufferedWriter(new FileWriter(fileOutput))
    bw.write(compact(render(json)))
    bw.close()

    spark.stop()
  }

  /**
    * Remove duplicate topics
    * @param names  Original topics (not yet split into list of topics)
    * @return   Set containing all different topics
    */
  def removeDuplicates (names: String): Set[String] = {

    val nameArray = names.split(";")
    val nameSet = collection.mutable.Set[String]()

    for (name <- nameArray) {
      nameSet.add(name.split(",")(0))
    }
    nameSet.toSet
  }

  /**
    * Flat the key-value pair to match key to every element in the value set.
    * The 1st entry is "DATE", the 2nd is "AllNames" and the 3rd is the number of occurances of each topic
    * @param day  "DATE"
    * @param nameSet  Set containing different topics in one event.
    * @return   Set("DATE", "AllNames", Number)
    */
  def flatMatch (day: String, nameSet: Set[String]): Set[Tuple3[String, String, Int]] = {

    val nameTuple = collection.mutable.Set[Tuple3[String, String, Int]]()

    for (name <- nameSet) {
      nameTuple.add((day, name, 1))
    }
    nameTuple.toSet
  }
}
