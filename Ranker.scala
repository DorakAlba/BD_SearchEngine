import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ranker{
  def main(args: Array[String]){ // maim func
    val conf = new SparkConf().setAppName("Sample App") // std things
    //conf.setMaster("local[4]") //для запуска на локалке

    val sc = new SparkContext(conf) // to read json
    val mySpark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import mySpark.implicits._ // function fot json

    //val inputFolder_Voc = "/Users/V/Desktop/BD_ass/Test6All/"
    //val outputFolder = "/Users/V/Desktop/BD_ass/Test7All/"
    val inputFolder_Voc = args(0)
    val outputFolder = args(1)

    // read vocabulary\
    //val query_string= List("how many years in day day anarchist")
    val query_string= List(args(2))

    val query_df = query_string.toDF()
    val voc = mySpark.read.json(inputFolder_Voc).toDF() // read json
    // read query

    //кол-во слов в каждом документе
    val len = voc.map(f => (f.getString(1),f.getLong(0))).rdd
      .reduceByKey(_ + _)
      .map(f => (f._1,f._2))

    //среднее значение
    val full_len = len.map(f => ("key",f._2))
      .reduceByKey(_ + _).map(f=>f._2).collect()
    val doc_count = len.map(f => ("key",1))
      .reduceByKey(_ + _).map(f=>f._2).collect()
    val ave = full_len(0)/doc_count(0)

    //  val query = mySpark.read.text(query_string) // DataFrame
    // preprocess query
    val pat1 = """[\p{Punct}]""" // remove all punctuation
    val pat2 = """ +""" // remove spaces
    val query_preprocessed = query_df.map(rows => rows.getString(0).toLowerCase()).rdd
      // .map(line => line.replaceAll(pat, ""))
      // .map(line => line.replaceAll(pat2, ""))
      .map(line => line.replaceAll(pat1, " "))
      .map(line => line.replaceAll(pat2, " "))
      .flatMap(line => line.split(" "))
    // Group similar words
    val query_group = query_preprocessed.toDF("text").groupBy("text").count().toDF("text","query_counts")

    val joined_voc = voc.join(query_group, Seq("text"),"inner") // join two dataframes by text column
      .withColumn("query_counts_tf-idf", col("query_counts") / col("idf")) // weights
      .withColumn("multiplication_tf-idf", col("query_counts_tf-idf") * col("tf-idf"))

    //result of multi ranker 1
    val result = joined_voc.map(f => (f.getString(2),f.getDouble(7))).rdd
      .reduceByKey(_ + _)
      .sortBy(- _._2)
      .map(f => f._1 +","+ f._2)

    //сохраняем в excel
    result.toDF()
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(outputFolder+""+query_string+".csv")

    //ranker 2
    val k = 2.0
    val b = 0.75
    val joined_voc2 = joined_voc.join(len.toDF("id","doc_len"), Seq("id"),"left")
      .withColumn("bm25_1", (col("idf")*col("count")*(k+1)))
      .withColumn("bm25_2", (((col("doc_len")*b/ave)-b+1)*k+col("count")))
      .withColumn("bm25",(col("bm25_1")/col("bm25_2")))

    //ранкируем и сортируем
    val result2 = joined_voc2.map(f => (f.getString(0),f.getDouble(11))).rdd
      .reduceByKey(_ + _)
      .sortBy(- _._2)
      .map(f => f._1 +","+ f._2)

    //сохраняем в excel2
    result2.toDF()
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(outputFolder+"bm"+query_string+".csv")

  }
}

