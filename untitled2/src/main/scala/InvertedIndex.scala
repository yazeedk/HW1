import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.ReadConfig
import java.io.{BufferedWriter, FileWriter, File}

object InvertedIndex{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("InvertedIndex")
      .setMaster("local[*]")
      .set("spark.mongodb.output.uri", "mongodb://localhost:27017/InvertedIndex.dictionary")
      .set("spark.mongodb.input.uri", "mongodb://localhost:27017/InvertedIndex.dictionary")

    val sc = new SparkContext(conf)

    val documents = sc.wholeTextFiles("C:\\Users\\yazee\\IdeaProjects\\untitled2\\DocsFiles\\dataset1.txt")

    val wordsInDocs = documents.flatMap {
      case (docPath, content) =>
        val docName = docPath.split("[\\\\/]").last // Support both Windows and Unix path separators
        content.split("\\W+").filter(_.nonEmpty).map(word => (word.toLowerCase, docName))
    }

    val wordDocPairs = wordsInDocs.distinct()

    val invertedIndexRDD = wordDocPairs
      .groupByKey()
      .mapValues { docs =>
        val docSet = docs.toSet
        val sortedDocs = docSet.toSeq.sorted
        (sortedDocs.size, sortedDocs.mkString(", "))
      }

    val sortedIndexRDD = invertedIndexRDD.sortByKey()

    val outputRDD = sortedIndexRDD.map { case (word, (count, docList)) =>
      s"$word, $count, $docList"
    }

    // Save to MongoDB
    val outputMongoDocuments: RDD[Document] = outputRDD.map { line =>
      val parts = line.split(", ", 3)
      new Document("word", parts(0))
        .append("count", parts(1).toInt)
        .append("documents", parts(2).split(", ").toList.asJava)
    }

    MongoSpark.save(outputMongoDocuments)

    // Save to text file
    val outputDir = "path_to_output"
    val outputFile = new File(outputDir, "inverted_index.txt")
    val bw = new BufferedWriter(new FileWriter(outputFile))

    outputRDD.collect().foreach { line =>
      bw.write(line)
      bw.newLine()
    }

    bw.close()

    sc.stop()

    val spark = SparkSession.builder()
      .appName("InvertedIndexQuery")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/InvertedIndex.dictionary")
      .getOrCreate()

    while (true) {
      println("Enter a query (or type 'exit' to quit):")
      val query = scala.io.StdIn.readLine()
      if (query.toLowerCase == "exit") {
        println("Exiting...")
        spark.stop()
        sys.exit(0)
      }
      queryProcessing(query, spark)
    }
  }

  private def queryProcessing(query: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/InvertedIndex.dictionary"))
    val mongoDF = MongoSpark.load(spark, readConfig)

    val queryWords = query.toLowerCase.split(" ")

    val filteredDF = mongoDF.filter(row => queryWords.contains(row.getAs[String]("word")))

    val result = filteredDF.select("documents").as[List[String]].collect().flatten.distinct

    if (result.nonEmpty) {
      println(s"Results for query '$query': ${result.mkString(", ")}")
    } else {
      println(s"No documents found for query '$query'")
    }
  }
}
