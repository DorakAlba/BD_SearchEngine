import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object MainClass{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Sample App")
    conf.setMaster("local")
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
    val outputPath = "/Users/V/Desktop/BD_ass/Test3"
    val outputFolder = "/Users/V/Desktop/BD_ass/Test6All"

    /*val readExample = mySpark.read.option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .json(inputPath)*/
    val regex2 = "(.*\\p{Punct}.*)" //регулярное выражение для удаления всех слов с пунктуацией

    /*читаем один файл, который состоит из нескольких элементов, считаем кол-во слов
    val readFull = mySpark.read.json(inputPath)
    readFull.printSchema()
    readFull.createOrReplaceTempView("Files")
    val data=mySpark.sql("select text from Files")
    //val d = readFull.select('text).collect().map(rows => rows.getString(0))//.map(line => line.replaceAll(regex2, ""))
    val ds = data.map(rows => rows.getString(0)).map(line => line.replaceAll(regex2, "")).rdd
      val counts = ds.flatMap(line => line.split(" "))
      //.filter(word => !(word matches regex2))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortBy(- _._2)
        .map(f => f._1 +"\t"+ f._2)
    counts.saveAsTextFile(outputPath)*/

    //считываем все файлы из папки в один dataset и считаем слова
    //по сути нам надо создать такой файл для каждой строки/документа отдельно
    //и создать вектор считающий в скольких документах содержится слово
    val readAll = mySpark.read.json(inputFolder)
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
      .map(f => f._1 +"\t"+ f._2)
    counts2.saveAsTextFile(outputFolder)

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
