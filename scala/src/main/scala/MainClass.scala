import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._

object MainClass{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Sample App")
    conf.setMaster("local[4]") //для запуска на локалке

    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val mySpark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import mySpark.implicits._

    val inputPath = "/Users/V/Desktop/BD_ass/files/AA_wiki_00"
    val inputFolder = "/Users/V/Desktop/BD_ass/files"
    val outputPath = "/Users/V/Desktop/BD_ass/Vocab"
    val outputFolder = "/Users/V/Desktop/BD_ass/Test6All"

    val regex2 = "(.*\\p{Punct}.*)" //регулярное выражение для удаления всех слов с пунктуацией
    val pat = """[^\w\s\.\$]"""
    val pat2 = """\s\w{2}\s"""
    val pat3 = """(\w)\.(?!\S)""" // remove dot after word

    val pats = (s:String) => {
      s.replaceAll(pat,null)
        .replaceAll(pat2,null)
        .replaceAll(pat3,null)
    }


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
      .sortBy(- _._2)
      .map(f => f._1 +"\t"+ f._2)
    vocab.saveAsTextFile(outputPath)

    //считаем TF
    val separate=mySpark.sql("select id, text from Files")
    //val d = readFull.select('text).collect().map(rows => rows.getString(0))//.map(line => line.replaceAll(regex2, ""))
    val dss = separate.map(rows => (rows.getString(0), rows.getString(1).toLowerCase().split(" "))).toDF("id","text")
    //val cc = ds.flatMap(line => line._2.split(" "))
    dss.printSchema()
    val dss2 = dss.withColumn("text", explode($"text"))
      .groupBy("id","text")
      .count()
      .sort(asc("id"),desc("count"))

    dss2.printSchema()

    //считаем IDF
    val dss3 = dss2.groupBy("text").agg(countDistinct("id") as "idf")
    dss3.printSchema()

    //объединяем таблицы по одинаковым словам
    val dss4 = dss2.join(dss3, Seq("text"), "left")
      .withColumn("tf-idf", col("count") / col("idf"))

    dss4.printSchema()
    val op = dss4.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile(outputFolder)



    //считываем все файлы из папки в один dataset и считаем слова
    //по сути нам надо создать такой файл для каждой строки/документа отдельно
    //и создать вектор считающий в скольких документах содержится слово

    /*val readAll = mySpark.read.json(inputFolder)
    readAll.printSchema()
    readAll.createOrReplaceTempView("Files")
    val data2=mySpark.sql("select text from Files")
    //val d = readFull.select('text).collect().map(rows => rows.getString(0))//.map(line => line.replaceAll(regex2, ""))
    val ds2 = data2.map(rows => rows.getString(0)).rdd
    val counts2 = ds2.flatMap(line => line.split(" "))
      .filter(word => !(word matches regex2)) //удаляем все слова с пунктуацией, если найдете норм способ удалить только пунктуацию будет круто, но необязательно
      //.map(line => line.replaceAll(regex2,"")) //работает не очень, вставляет пустые символы
      //.filter(word => !(word matches ""))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(- _._2)
      .map(f => f._1 +"\t"+ f._2)*/

    //counts2.saveAsTextFile(outputFolder)



    /* другой способ
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json(inputPath)
    df.printSchema()
    df.registerTempTable("JSONdata")
    val data=sqlContext.sql("select title from JSONdata")
    sc.stop*/
    //val inputPath = args(0)
    //val rawJson: String = {"foo": "bar","baz": 123,"list of stuff": [ 4, 5, 6 ]}"""
    //val parseResult = parse(rawJson).getOrElse(Json.Null)
    //println(parseResult)
  }
}
