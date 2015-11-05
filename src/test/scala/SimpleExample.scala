import org.apache.spark.{SparkConf, SparkContext}

object SimpleExample extends App{

  var conf = new SparkConf()
  conf.setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //Write
  var df = sqlContext.read.format("org.apache.spark.sql.xml").option("rootTag", "book").load("hdfs://localhost:9000/user/hyukjinkwon/books.xml")
  df.registerTempTable("test")

  val result_df = sqlContext.sql(
    """
         SELECT *
         FROM test
    """
  )
  result_df.printSchema()
  result_df.collect().foreach(println)
}
