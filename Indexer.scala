import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._

object Indexer{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Sample App")
    //conf.setMaster("local[4]") //для запуска на локалке

    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val mySpark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      //.master("local[4]")
      .getOrCreate()
    import mySpark.implicits._

    //теперь читаем аргументы вместо пути
    //val inputPath = "/Users/V/Desktop/BD_ass/files/AA_wiki_00"
    //val inputFolder = "/Users/V/Desktop/BD_ass/files"
    //val outputPath = "/Users/V/Desktop/BD_ass/Vocab"
    //val outputFolder = "/Users/V/Desktop/BD_ass/Test6All"

    val inputPath = args(0)
    val outputFolder = args(1)

    val regex2 = "(.*\\p{Punct}.*)" //регулярное выражение для удаления всех слов с пунктуацией
    val pat = """[^\w\s\.\$]"""
    val pat2 = """\s\w{2}\s"""
    val pat3 = """(\w)\.(?!\S)""" // remove dot after word


    //читаем один файл, который состоит из нескольких элементов, считаем кол-во слов
    val readFull = mySpark.read.json(inputPath)
    readFull.printSchema()
    readFull.createOrReplaceTempView("Files")

    //считаем все слова
    val full=mySpark.sql("select text from Files")
    val vocab = full.map(rows => rows.getString(0).toLowerCase()).rdd
      .flatMap(line => line.split(" "))
      .map(line => line.replaceAll(pat, ""))
      .map(line => line.replaceAll(pat2, ""))
      .map(line => line.replaceAll(pat3, ""))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      //.sortBy(- _._2) убираем для оптимизации
      .map(f => f._1 +"\t"+ f._2)
    //vocab.saveAsTextFile(outputFolder)

    //считаем TF
    val separate=mySpark.sql("select id, text from Files")
    //val d = readFull.select('text).collect().map(rows => rows.getString(0))//.map(line => line.replaceAll(regex2, ""))
    //добавили replace
    val dss = separate.map(rows => (rows.getString(0), rows.getString(1).toLowerCase().split(" ").map(line => line.replaceAll(pat, "")).map(line => line.replaceAll(pat2, "")).map(line => line.replaceAll(pat3, "")))).toDF("id","text") //  (id, text) name of columns

    //val dss = separate.map(rows => (rows.getString(0), rows.getString(1).toLowerCase().split(" "))).toDF("id","text")
    //val cc = ds.flatMap(line => line._2.split(" "))
    //dss.printSchema()
    val dss2 = dss.withColumn("text", explode($"text"))
      .groupBy("id","text")
      .count()
      //.sort(asc("id"),desc("count")) убираем для оптимизации

    //dss2.printSchema()

    //считаем IDF
    val dss3 = dss2.groupBy("text").agg(countDistinct("id") as "idf")
    //dss3.printSchema()

    //объединяем таблицы по одинаковым словам
    val dss4 = dss2.join(dss3, Seq("text"), "left")
      .withColumn("tf-idf", col("count") / col("idf"))

    //dss4.printSchema()
    //val op = dss4.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile(outputFolder)
    dss4.write.json(outputFolder)

  }
}
