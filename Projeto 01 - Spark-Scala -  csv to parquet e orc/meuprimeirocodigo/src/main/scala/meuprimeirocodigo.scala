import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, mean}
import org.apache.log4j.Logger

object meuprimeirocodigo {
  import org.apache.log4j.Level
  Logger.getLogger( "org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit ={
    val spark  = SparkSession
      .builder
      .appName( name= "meuprimeirocodigo")
      .master( master = "local[*]") // comentar quando for no cluster
      .getOrCreate()

    val df = spark.read.format(source = "csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(path = "/home/clayton/Downloads/weekly_MSFT.csv")
     // .load(path = "s3/home/clayton/Downloads/weekly_MSFT.csv")


    df.show(truncate = false)

    df.printSchema()

    df.select(mean(columnName = "close") as("mediana")).show()
    df.select(max(columnName = "close") as("maior")).show()

    df.agg(mean(columnName = "close")).show()

    // exhibition plan execution
    df.select(mean(columnName = "close")).explain()

    df.agg(mean(columnName = "close")).explain()

    // ----transform csv to parquet----
    df.write.parquet(path = "/home/clayton/Downloads/trusted/parquet")
    //df.write.parquet(path = "/home/clayton-trusted/dolar/parquet")

    // ---transform csv to orc----
    df.write.orc(path = "/home/clayton/Downloads/trusted/orc")
    //df.write.orc(path = "s3://clayton-trusted/dolar/orc")
  }

}
